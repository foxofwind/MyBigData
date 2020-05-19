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
public class RepayDomeTest {
    public static final String INPUT_TOPIC = "lloantest1";
    public static final String OUTPUT_TOPIC = "repay_dome_hubg";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RepayDome"); //groupId
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.133:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest  earliest
        return props;
    }

    private static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final KTable<String, String> ksFo = source
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
//                    String vType = v.getType();
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
                .groupByKey()
                .aggregate(
                        new Initializer<String>() {
                            @Override
                            public String apply() {
                                return JSON.toJSONString(new HashMap<String, RepayPlanCustTemp>());
                            }
                        },
                        new Aggregator<String, String, String>() {
                            @Override
                            public String apply(String aggKey, String newValue, String aggValue) {
//                                System.out.println("aggKey:" + aggKey + ",  newValue:" + newValue + ", aggKey:" + aggValue);
                                RepayPlanCustSource oin = JSON.parseObject(newValue, new TypeReference<RepayPlanCustSource>() {
                                });
                                Map<String, RepayPlanCustTemp> res = JSON.parseObject(aggValue
                                        , new TypeReference<Map<String, RepayPlanCustTemp>>() {
                                        });
                                RepayPlanCustTemp tmp = getTempFromSource(oin);
                                if (tmp.getIS_DELETED().equals("1")) {
                                    res.remove(tmp.getCurrent_repay_period());
                                } else {
                                    res.put(tmp.getCurrent_repay_period(), tmp);
                                }
                                return JSON.toJSONString(res);
                            }
                        }
                );
        KStream<String, String> outString = ksFo.toStream()
                .map(
                        new RepayOutMapper()
                );
        outString.map((k, v) -> {
            return new KeyValue<String, String>(k, v.toString());
        }).print(Printed.<String, String>toSysOut().withLabel("Stocks-KTable"));
        outString.map((k, v) -> {
            return new KeyValue<String, String>(k, JSON.toJSONString(v));
        }).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static RepayPlanCustTemp getTempFromSource(RepayPlanCustSource f) {
        RepayPlanCustTemp o = new RepayPlanCustTemp();
        o.setDbDMLType(f.getType());
        if (f.getApply_no() != null) {
            o.setApply_no(f.getApply_no());
        }
        if (f.getID() != null) {
            o.setID(f.getID());
        }
        if (f.getPlan_repay_date() != null) {
            if (f.getPlan_repay_date().length() == 8) {
                String planRepay_date = f.getPlan_repay_date().substring(0, 4)
                        + "-" + f.getPlan_repay_date().substring(4, 6)
                        + "-" + f.getPlan_repay_date().substring(6, 8);
                o.setPlan_repay_date(planRepay_date);
            } else {
                o.setPlan_repay_date(f.getPlan_repay_date());
            }
        }

        if (f.getCurrent_repay_period() != null) {
            o.setCurrent_repay_period(f.getCurrent_repay_period());
        }

        if (f.getActual_repay_date() != null) {
            if (f.getActual_repay_date().length() == 8) {
                String actual_repay_date = f.getActual_repay_date().substring(0, 4)
                        + "-" + f.getActual_repay_date().substring(4, 6)
                        + "-" + f.getActual_repay_date().substring(6, 8)
                        + " 00:00:00";
                o.setActual_repay_date(actual_repay_date);
            } else if (f.getActual_repay_date().length() == 10) {
                o.setActual_repay_date(f.getActual_repay_date() + " 00:00:00");
            } else {
                o.setActual_repay_date(f.getActual_repay_date());
            }
        }

        if (f.getPlan_repay_principal() != null) {
            o.setPlan_repay_principal(f.getPlan_repay_principal());
        }

        if (f.getPlan_repay_interest() != null) {
            o.setPlan_repay_interest(f.getPlan_repay_interest());
        }

        if (f.getOverdue_penalty_money() != null) {
            o.setOverdue_penalty_money(f.getOverdue_penalty_money());
        }

        if (f.getRepay_state() != null) {
            o.setRepay_state(f.getRepay_state());
        }

        if (f.getActual_repay_total_money() != null) {
            o.setActual_repay_total_money(f.getActual_repay_total_money());
        }
        if (f.getPlan_repay_total_money() != null) {
            o.setPlan_repay_total_money(f.getPlan_repay_total_money());
        }
        if (f.getOverdue_days() != null) {
            o.setOverdue_days(f.getOverdue_days());
        }

        if (f.getOverdue_days() != null) {
            if (f.getOverdue_days().compareTo("0") > 0) {
                o.setIs_overdue_his(1);
            } else {
                o.setIs_overdue_his(0);
            }
        }

        if (f.getRepay_state() != null) {
            if (f.getRepay_state() != null) {
                if (f.getRepay_state().equals("2")
                        || f.getRepay_state().equals("5")
                        || f.getRepay_state().equals("16")
                        || f.getRepay_state().equals("20")
                        || f.getRepay_state().equals("8")
                        || f.getRepay_state().equals("3")
                        || f.getRepay_state().equals("19")

                ) {
                    o.setIs_alrepaid(1);
                } else {
                    o.setIs_alrepaid(0);
                }
            } else {
                o.setIs_alrepaid(0);
            }


            if (f.getRepay_state() != null) {
                if (!f.getRepay_state().equals("2")
                        && !f.getRepay_state().equals("5")
                        && !f.getRepay_state().equals("16")
                        && !f.getRepay_state().equals("20")
                        && !f.getRepay_state().equals("8")
                        && !f.getRepay_state().equals("3")
                        && !f.getRepay_state().equals("19")
                        && !f.getRepay_state().equals("1")
                ) {
                    o.setIs_overdue_cur(1);
                } else {
                    o.setIs_overdue_cur(0);
                }
            } else {
                o.setIs_overdue_cur(0);
            }
        }
        if (f.getIS_DELETED() != null) {
            o.setIS_DELETED(f.getIS_DELETED());
        }
        return o;
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
