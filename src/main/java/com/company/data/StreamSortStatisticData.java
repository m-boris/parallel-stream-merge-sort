package com.company.data;

public class StreamSortStatisticData {

    private int numberOfChunks;
    private int numberOfLines;

    public StreamSortStatisticData(int numberOfChunks, int numberOfLines) {
        this.numberOfChunks = numberOfChunks;
        this.numberOfLines = numberOfLines;
    }

    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    public int getNumberOfLines() {
        return numberOfLines;
    }
}
