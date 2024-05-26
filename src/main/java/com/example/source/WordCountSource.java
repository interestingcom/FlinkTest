package com.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Random;

public class WordCountSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    private String[] words = {"hello world","hello people","hello flink","hello spark"};
    private Random random = new Random();
    private SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning){
            String datetime = tempDate.format(new java.util.Date());
            String data = words[random.nextInt(100)%words.length];
            System.out.println(datetime+"===写入===>"+data);
            ctx.collect(data);
            Thread.sleep(5*1000L);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}