package com.company;

import com.company.data.LineData;
import com.company.data.StreamSortStatisticData;
import com.company.util.FileSortUtils;
import com.company.worker.QueueToWriter;
import com.company.worker.SortedChunkWriter;
import com.company.worker.StreamProducer;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelStreamSorterImpl implements  StreamSorter {

    @Override
    public int sortedChunkMerger(String processingFolder, long numOfChunks, String output, int chunkSize) throws IOException, InterruptedException, ExecutionException {
        FileWriter writer = null;
        ArrayBlockingQueue<LineData> queue = new ArrayBlockingQueue<>( chunkSize );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<StreamProducer> inputs = null;
        int numberOfWroteLines = 0;
        try{
            writer = new FileWriter( output );
            inputs = init(processingFolder, numOfChunks);
            AtomicBoolean isProducerActive = new AtomicBoolean( !inputs.isEmpty() );
            QueueToWriter queueToWriter = new QueueToWriter( queue, isProducerActive, writer );

            Future<Integer> f = executor.submit( queueToWriter );
            LineData lineData;
            do {
                lineData = null;
                Optional<StreamProducer> o = inputs.stream().filter(sp -> sp.getCurrentLineData() != null).
                        min((sp1, sp2) ->
                                StringUtils.compare(sp1.getCurrentLineData().getValue(), sp2.getCurrentLineData().getValue()));
                if (o.isPresent()) {
                    lineData = o.get().popLineData();
                    queue.put( lineData );
                }
            }while ( lineData!=null );
            isProducerActive.set(false);
            numberOfWroteLines = f.get();
            System.out.println( "Number of lines in result file " + numberOfWroteLines );
        } finally {
            executor.shutdown();
            if ( inputs!=null ){
                inputs.forEach( i -> i.shutdown() );
            }
            if ( writer!=null ) {
                writer.flush();
                writer.close();
            }
        }
        return numberOfWroteLines;
    }

    private List<StreamProducer> init(String processingFolder, long numOfChunks) throws IOException {
        List<StreamProducer> inputs = new ArrayList<>();
        for ( int i=0;i<numOfChunks;i++ ) {
            String fileName = FileSortUtils.getProcessingFileName( processingFolder, i );
            StreamProducer sp = new StreamProducer( fileName );
            sp.init();
            inputs.add( sp );
        }
        return inputs;
    }



    @Override
    public StreamSortStatisticData sortedChunkSplitter(String input, String processingFolder, int chunkSize) throws IOException {
        int chunkNumber = 0;
        List< LineData > list;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        int numberOfLines = 0;
        try ( BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream( input )))){
            Future<Integer> f = null;
            do{
                list = readChunk( fileInputStream, chunkSize );
                if ( list!=null && !list.isEmpty() ) {
                    numberOfLines+=list.size();
                    SortedChunkWriter sortedChunkWriter = new SortedChunkWriter( list, chunkNumber, processingFolder );
                    // wait for previuos process done to avoid data size > 2*chunkSize
                    waitForFutureDone(f);
                    chunkNumber++;
                    f = executor.submit( sortedChunkWriter );
                }
            }while ( list!=null && !list.isEmpty() );
            waitForFutureDone(f);
        } finally {
            executor.shutdown();
        }
        return  new StreamSortStatisticData( chunkNumber, numberOfLines );
    }

    private void waitForFutureDone(Future<Integer> f) {
        if ( f!=null){
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }


    private List<LineData> readChunk(BufferedReader fileInputStream, long chunkSize) throws IOException {
        String line;
        List<LineData> lineData = new ArrayList<>();
        while ( chunkSize>0  && (line = fileInputStream.readLine()) != null ){
            if ( LineData.isLineData( line ) ) {
                lineData.add(new LineData(line));
                chunkSize--;
            }
        }
        return lineData;
    }
}
