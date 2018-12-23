package com.company.worker;

import com.company.data.LineData;
import com.company.util.FileSortUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class SortedChunkWriter implements Callable<Integer> {

    private List<LineData> list;
    private int chunkId;
    private String outputFolder;

    public SortedChunkWriter(List<LineData> lineData, int chunkId, String outputFolder) {
        this.list = lineData;
        this.chunkId = chunkId;
        this.outputFolder = outputFolder;
    }

    @Override
    public Integer call() throws Exception {
        sortList( list );
        saveChunk( list, FileSortUtils.getProcessingFileName(outputFolder, chunkId));
        return chunkId;
    }

    private void sortList(List<LineData> list) {
        list.sort( (line1, line2) -> StringUtils.compare( line1.getValue(), line2.getValue() ) );

    }

    private static void saveChunk(List<LineData> list, String outputFile ) throws IOException {
        try( FileWriter writer = new FileWriter( outputFile )){
            for(LineData lineData: list) {
                FileSortUtils.writeLineData(writer, lineData);
            }
        }
    }
}
