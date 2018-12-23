package com.company;

import com.company.data.StreamSortStatisticData;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * boris.freidberg@gmail.com
 * please send c:/workspace folder also
 */
public class Main {

    private static final int CHUNK_SIZE = 25;
    private static final String BASE_FOLDER = "C:/workspace";

    private static final String INPUT_FILE = BASE_FOLDER + "/input/unsorted-input.csv";
    private static final String PROCESSING_FOLDER = BASE_FOLDER + "/inprocess";
    private static final String OUTPUT_FILE = BASE_FOLDER + "/output/sorted-output.csv";

    public static void main(String[] args) {
        System.out.println( "Usage:" );
        System.out.println( "Program required 3 arguments:  " );
        System.out.println( "input file, processing folder, output file " );
        String inputFile = INPUT_FILE;
        String outputFile = OUTPUT_FILE;
        String processingFolder = PROCESSING_FOLDER;
        if ( args.length!=3 ){
            System.out.printf( "The arguments not provided use defaults %s %s %s:\n", inputFile, outputFile, processingFolder );
        } else {
            inputFile = args[0];
            outputFile = args[1];
            processingFolder = args[2];
        }
        int status = executeMergeSort( inputFile, outputFile, processingFolder );
        System.exit( status );
    }

    private static int executeMergeSort( String inputFile, String outputFile, String processingFolder ) {
        int status = 1;
        try{
            boolean isSuccess = streamSorter( inputFile, outputFile, processingFolder  );
            if ( isSuccess ) {
                System.out.println("Success");
                status = 0;
            } else {
                System.out.println("Fail!!! number of input lines different of number of output lines");
            }
        }catch (IOException | ExecutionException | InterruptedException e ){
            e.printStackTrace();
            System.out.println( "FAIL!!!" );
        }
        return status;
    }

    private static boolean streamSorter(String input, String output, String processingFolder ) throws IOException, ExecutionException, InterruptedException {
        StreamSorter streamSorter = new ParallelStreamSorterImpl();
        StreamSortStatisticData sd = streamSorter.sortedChunkSplitter( input, processingFolder, CHUNK_SIZE );
        int numberOfWroteLines = streamSorter.sortedChunkMerger(processingFolder, sd.getNumberOfChunks(), output, CHUNK_SIZE);
        System.out.printf( "Number of read lines: %d, number of wrote lines %d\n", sd.getNumberOfLines(), numberOfWroteLines );
        // the ideal test is check the output file has value in right order and input size equals to output size
        // for simplicity I check if the number of input lines == number of output lines
        return sd.getNumberOfLines()==numberOfWroteLines;
    }


}
