package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yixin.hubg.domain.Statistics;
import com.yixin.hubg.domain.StatisticsDeserializer;
import com.yixin.hubg.domain.StatisticsSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 统计60秒内，温度值的最大值  topic中的消息格式为数字，30， 21或者{"temp":19, "humidity": 25}
 */
public class TemperatureAvgDemo {
    private static final int TEMPERATURE_WINDOW_SIZE = 60;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temp-avg");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //流构造器
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("iot-temp");
        KTable<Windowed<String>, Statistics> max = source
                .selectKey(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String value) {
                        return "stat";
                    }
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(TEMPERATURE_WINDOW_SIZE)))
                .aggregate(
                        new Initializer<Statistics>() {
                            @Override
                            public Statistics apply() {
                                Statistics avgAndSum = new Statistics(0L,0L,0L);
                                return avgAndSum;
                            }
                        },
                        new Aggregator<String, String, Statistics>() {
                            @Override
                            public Statistics apply(String aggKey, String newValue, Statistics aggValue) {
                                //topic中的消息格式为{"temp":19, "humidity": 25}
                                System.out.println("aggKey:" + aggKey + ",  newValue:" + newValue + ", aggKey:" + aggValue);
                                Long newValueLong = null;
                                try {
                                    JSONObject json = JSON.parseObject(newValue);
                                    newValueLong = json.getLong("temp");
                                }
                                catch (ClassCastException ex) {
                                    newValueLong = Long.valueOf(newValue);
                                }

                                aggValue.setCount(aggValue.getCount() + 1);
                                aggValue.setSum(aggValue.getSum() + newValueLong);
                                aggValue.setAvg(aggValue.getSum() / aggValue.getCount());

                                return aggValue;
                            }
                        },
                        Materialized.<String, Statistics, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-temp-stream-store")
                                .withValueSerde(Serdes.serdeFrom(new StatisticsSerializer(), new StatisticsDeserializer()))
                );

        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(Serdes.String().serializer());
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(Serdes.String().deserializer(), TEMPERATURE_WINDOW_SIZE);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        max.toStream().to("iot-temp-stat", Produced.with(windowedSerde, Serdes.serdeFrom(new StatisticsSerializer(), new StatisticsDeserializer())));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);


        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}