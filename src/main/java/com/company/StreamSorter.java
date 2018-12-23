package com.company;

import com.company.data.StreamSortStatisticData;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Boris Freidberg
 * boris.freidberg@gmail.com
 */
public interface StreamSorter {
    /**
     * The method split the input file to chunks sort the data for each chunk
     * and store it in processing folder
     * @param inputFileName
     * @param processingFolder
     * @param chunkSize
     * @return the  StreamSortStatisticData object contain number of chunks for continue processing and
     * number of read lines for result validation
     *
     * @throws IOException
     */
    StreamSortStatisticData sortedChunkSplitter(String inputFileName, String processingFolder, int chunkSize) throws IOException;

    /**
     * The method read the sorted files from processing folder merge it and save the sorted data into output
     * filename
     * @param processingFolder
     * @param numOfChunks
     * @param output
     * @param chunkSize
     * @return number of write lines for result validation
     * @throws IOException
     */
    int sortedChunkMerger(String processingFolder, long numOfChunks, String output, int chunkSize) throws IOException, InterruptedException, ExecutionException;
}
