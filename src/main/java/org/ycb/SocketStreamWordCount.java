package org.ycb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        // todo 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // todo 2. 读取文本遛：hadoop101 表示发送端主机名、7777表示端口号
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop101",7777);

        // todo 3. 切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        }).keyBy(0).sum(1);

        // todo 4. 打印
        sum.print();

        // todo 5. 执行
        env.execute();


    }
}
