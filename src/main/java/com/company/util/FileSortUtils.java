package com.company.util;

import com.company.data.LineData;

import java.io.FileWriter;
import java.io.IOException;

public class FileSortUtils {

    public static void writeLineData(FileWriter writer, LineData lineData) throws IOException {
        writer.write(lineData.getLineAsString());
        writer.append(System.lineSeparator());
    }

    public static String getProcessingFileName(String processingFolder, int chunkNumber) {
        return processingFolder+"/p"+chunkNumber+".csv";
    }

}
