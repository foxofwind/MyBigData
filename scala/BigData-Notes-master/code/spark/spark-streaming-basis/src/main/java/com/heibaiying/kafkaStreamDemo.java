package com.heibaiying;

/*import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class kafkaStreamDemo {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.137:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream("1");
        //对value进行操作，构造一个ValueMapper kafkaStream
        final KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                //数据格式:java,scala,python,c
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(","));//按照逗号切割，并变为集合
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, ?>>() {
            @Override
            public KeyValue<String, String> apply(String key, String value) {//只取value,按照单词进行分组
                return new KeyValue<>(value, value);
            }
        }).groupByKey().count("countstore");
        counts.print();
        final KafkaStreams streams = new KafkaStreams(builder, props);

        //启动与关闭,开启一个任务执行
        final CountDownLatch latch = new CountDownLatch(1);

        //线程完毕以后释放流
        Runtime.getRuntime().addShutdownHook(new Thread("word-count") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();//流关闭的同时，latch值变为0
            }
        });

        try {
            streams.start();
            latch.await();//线程被挂起,等待latch的值变为0才重新开始执行
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (StreamsException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}*/
