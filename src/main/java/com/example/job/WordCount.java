package com.example.job;

import utils.FlinkEnvironmentFactory;
import com.example.source.WordCountSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class WordCount {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment streamingEnvironment = FlinkEnvironmentFactory.createStreamingEnvironment();
        streamingEnvironment.setParallelism(4);

        SingleOutputStreamOperator<String> word = streamingEnvironment.addSource(new WordCountSource()).flatMap(new SplitMapFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = word.map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = wordAndOne.keyBy(value -> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);

        sum.print("flink计算结果===>");

        streamingEnvironment.execute();


    }


    private static class SplitMapFunction implements FlatMapFunction<String, String> {


        @Override
        public void flatMap(String input, Collector<String> collector) throws Exception {
            String[] strings = input.split(" ");
            Random random = new Random();
            int randomInt = random.nextInt(55);
            for (String string : strings) {
                collector.collect(string);

                if ( string.equals("world") && randomInt %5==0){
                    throw new Exception("随机数是randomInt"+randomInt+"遇到单词world...");
                }

            }
        }
    }


}
