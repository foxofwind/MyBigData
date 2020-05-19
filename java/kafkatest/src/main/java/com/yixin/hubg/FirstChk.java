package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.KafkaData;
import com.yixin.hubg.domain.RepayPlanCustSource;
import com.yixin.hubg.domain.RepayPlanCustTemp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 */
public class FirstChk {
    public static final String INPUT_TOPIC = "lloantest1";
    public static final String OUTPUT_TOPIC = "FirstChk";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstChk"); //groupId
        //10.0.4.43:9091,10.0.4.44:9091,10.0.4.45:9091
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.4.43:9091,10.0.4.44:9091,10.0.4.45:9091");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest  earliest
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractorImpl.class.getName());
//        System.out.println(TimestampExtractorImpl.class.getName());
        return props;
    }

    private static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final KStream<String, String> ksFo = source
                .map((k, v) -> {
                    KafkaData<RepayPlanCustSource> sdata = JSON.parseObject(v,
                            new TypeReference<KafkaData<RepayPlanCustSource>>() {
                            });
                    return new KeyValue<String, KafkaData<RepayPlanCustSource>>(k, sdata);
                })
                .filter((k, v) -> v != null && v.getTable().toLowerCase().equals("fa_repay_plan_cust") &&
                        v.getData() != null && v.getData().size() > 0)
                .flatMap((k, v) -> {
                    List<KeyValue<String, String>> keyValues = new ArrayList<>();
                    for (RepayPlanCustSource fs : v.getData()) {
                        fs.setDatabase(v.getDatabase());
                        fs.setId(v.getId());
                        fs.setTable(v.getTable());
                        fs.setTs(v.getTs());
                        fs.setType(v.getType());
                        keyValues.add(new KeyValue<String, String>(fs.getApply_no(), JSON.toJSONString(fs)));
                    }
                    return keyValues;
                })
                .filter((k, v) -> k != null && k.equals("1000396049"));
        ksFo.print(Printed.<String, String>toSysOut().withLabel("Stocks-KTable"));
        ksFo.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void main(String[] args) {
        final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("rt") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
