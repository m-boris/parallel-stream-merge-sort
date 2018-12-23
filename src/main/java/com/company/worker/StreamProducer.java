package com.company.worker;

import com.company.data.LineData;

import java.io.*;

public class StreamProducer {

    private String fileName;
    private BufferedReader inputStream;
    private boolean isEndOfStream = false;
    private LineData currentLineData = null;

    public StreamProducer( String fileName ) throws FileNotFoundException {
        this.fileName = fileName;
    }

    public void init() throws IOException{
        inputStream = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
        currentLineData = readLineData( inputStream );
    }

    public LineData getCurrentLineData() {
        return currentLineData;
    }

    public LineData popLineData() throws IOException {
        LineData current = this.currentLineData;
        this.currentLineData = readLineData( inputStream );
        return current;
    }

    public void shutdown(){
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private LineData readLineData(BufferedReader inputStream) throws IOException {
        String line = null;
        do{
            line = inputStream.readLine();
            if ( line==null ) {
                isEndOfStream = true;
            }
        }while ( !isEndOfStream && !LineData.isLineData( line ) );
        LineData lineData = null;
        if ( !isEndOfStream ){
            lineData = new LineData( line );
        }
        return lineData;
    }

}
