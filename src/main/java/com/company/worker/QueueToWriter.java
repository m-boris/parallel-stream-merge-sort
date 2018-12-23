package com.company.worker;

import com.company.data.LineData;
import com.company.util.FileSortUtils;

import java.io.FileWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueToWriter implements Callable<Integer> {

    private ArrayBlockingQueue<LineData> queue;
    private AtomicBoolean isProducersActive;
    private FileWriter writer;

    public QueueToWriter(ArrayBlockingQueue<LineData> queue, AtomicBoolean isProducersActive, FileWriter writer) {
        this.queue = queue;
        this.isProducersActive = isProducersActive;
        this.writer = writer;
    }

    @Override
    public Integer call() throws Exception {
        int lineCount = 0;
        LineData lineData = null;
        do{
            lineData = queue.poll( 10, TimeUnit.MILLISECONDS );
            if ( lineData!=null ){
                FileSortUtils.writeLineData( writer, lineData );
                lineCount++;
            }
        }while( isProducersActive.get() || lineData!=null );
        return lineCount;
    }
}
