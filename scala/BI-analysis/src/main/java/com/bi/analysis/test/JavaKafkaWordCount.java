package com.bi.analysis.test;
import java.util.*;
import java.util.regex.Pattern;

import com.bi.analysis.conf.ConfigurationManager;
import com.bi.analysis.constant.Constant;
import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;
/**
 * Created by Administrator on 2017/12/8.
 */
public class JavaKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    private JavaKafkaWordCount() { }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWord Count");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

//        int numThreads = Integer.parseInt(args[3]);

        String topic = ConfigurationManager.getProperty(Constant.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = topic.split(",");
        Set<String> topics = new HashSet<String>();
        for (String t: kafkaTopicsSplited) {
            topics.add(t);
        }
        System.out.println("==========="+topic);
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constant.KAFKA_METADATA_BROKER_LIST));
        kafkaParams.put("auto.offset.reset","smallest");

//        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topics
//                );

//        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple2) throws Exception {
//                return tuple2._2;
//            }
//        });
//
//        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
//            @Override
//            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
//                return   Lists.newArrayList(SPACE.split(tuple2._2)).iterator();
//            }
//        });
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//                new PairFunction<String, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(String s) throws Exception {
//                        return new Tuple2<String, Integer>(s, 1);
//                    }
//                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer v1, Integer v2) throws Exception {
//                        return v1+v2;
//                    }
//                });
//
//        wordCounts.print();

        jssc.start();
        //wordCounts.saveAsHadoopFiles("jwc", "sufix"); jssc.start();
//        jssc.awaitTermination();

    }
}
