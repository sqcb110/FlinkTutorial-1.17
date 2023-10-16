package org.ycb;


import com.sun.codemodel.internal.JVar;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // todo 1. 创建执行环境(流式环境)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // todo 2. 读取数据(一样的)
        DataStreamSource<String> linesStream = env.readTextFile("input/words.txt");

        // todo 3. 处理数据（切分，转换，分组，聚合）
        // todo 3.1 切分，转换
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = linesStream.flatMap( new FlatMapFunction<String, Tuple2<String, Long>>() {

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        }).keyBy(0).sum(1);



        // todo 4. 输出（打印）
        sum.print();

        // todo 5. 执行
        env.execute();

    }
}
