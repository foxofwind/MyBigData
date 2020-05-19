package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KtableTest {

    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.133:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
//        Serde<OrderIn> orderInSerde = Serdes.serdeFrom(new OrderInSerializer(), new OrderInDeserializer());
//        Serde<String> stringSerde = Serdes.String();
        KStream<String, String> textLines = builder.stream("input");
//        KStream<String, OrderIn> sds = textLines.map((k, v) -> {
//            String[] sdata = v.split(",");
//            OrderIn oi = new OrderIn(sdata[0],sdata[1] + sdata[2],sdata[1],Integer.valueOf(sdata[2]),Integer.valueOf(sdata[3]));
//            return new KeyValue<String, OrderIn>(sdata[1], oi);
//        });
        KStream<String, String> inStream = textLines
                .map((k, v) -> {
                    String[] sdata = v.split(",");
                    OrderIn oi = new OrderIn(sdata[0], sdata[1] + sdata[2], sdata[1], Integer.valueOf(sdata[2]), Integer.valueOf(sdata[3]));
                    return new KeyValue<String, String>(sdata[1], JSON.toJSONString(oi));
                });

        inStream.print(Printed.<String, String>toSysOut().withLabel("Stocks-KStream"));

        KTable<String, String> inKTable = inStream
                .groupByKey()
                .aggregate(
                        new Initializer<String>() {
                            @Override
                            public String apply() {
                                return JSON.toJSONString(new HashMap<Integer, OrderIn>());
                            }
                        },
                        new Aggregator<String, String, String>() {
                            @Override
                            public String apply(String aggKey, String newValue, String aggValue) {
//                                System.out.println("aggKey:" + aggKey + ",  newValue:" + newValue + ", aggKey:" + aggValue);
                                OrderIn oin = JSON.parseObject(newValue, new TypeReference<OrderIn>() {
                                });
                                Map<Integer, OrderIn> res = JSON.parseObject(aggValue
                                        , new TypeReference<Map<Integer, OrderIn>>() {
                                        });
                                res.put(oin.getPeriod(), oin);

                                return JSON.toJSONString(res);
                            }
                        }
                );
//        inKTable.toStream().print(Printed.<String, String>toSysOut().withLabel("Stocks-KTable"));
        KStream<String, String> outString = inKTable.toStream()
                .map(
                        new OutMapper()
                );



//                .groupByKey()
//                .aggregate(
//                        new Initializer<String>() {
//                            @Override
//                            public String apply() {
//                                return "";
//                            }
//                        },
//                        new Aggregator<String, String, String>() {
//                            @Override
//                            public String apply(String aggKey, String newValue, String aggValue) {
////                                System.out.println("aggKey:" + aggKey + ",  newValue:" + newValue + ", aggKey:" + aggValue);
//                                Map<Integer, OrderIn> nv = JSON.parseObject(newValue
//                                        , new TypeReference<Map<Integer, OrderIn>>() {
//                                        });
//                                OrderOut ret = new OrderOut("", 0, 0, 0, 0);
//                                ret.setSqbh(aggKey);
//                                if (ret == null) {
//                                    ret = new OrderOut();
//                                    ret.setSqbh(aggKey);
//                                }
//                                for (Map.Entry<Integer, OrderIn> entry : nv.entrySet()) {
//                                    Integer money = entry.getValue().getMoney();
//                                    if (ret.getOMin() == 0 || money < ret.getOMin()) {
//                                        ret.setOMin(money);
//                                    }
//                                    if (money > ret.getOMax()) {
//                                        ret.setOMax(money);
//                                    }
//                                    ret.setOSum(ret.getOSum() + money);
//                                    if (entry.getValue().getPeriod() == 1)
//                                        ret.setOFirst(money);
//                                }
//                                return JSON.toJSONString(ret);
//                            }
//                        }
//                );
        outString.map((k, v) -> {
            return new KeyValue<String, String>(k, v.toString());
        }).print(Printed.<String, String>toSysOut().withLabel("Stocks-KTable"));

//        outKTable.toStream().print(Printed.<String, Long>toSysOut().withLabel("Stocks-KTable"));
//        sds.print(Printed.<String, String[]>toSysOut().withLabel("Stocks-KStream"));

//                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//                wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}