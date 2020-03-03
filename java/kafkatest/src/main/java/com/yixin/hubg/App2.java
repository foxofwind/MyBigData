package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.FaApplyInfoExtOut;
import com.yixin.hubg.domain.FaApplyInfoExtSource;
import com.yixin.hubg.domain.KafkaData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 */
public class App2 {
    public static final String INPUT_TOPIC = "yx_mysql_lloan";
    public static final String OUTPUT_TOPIC = "fa_apply_info_ext_hubg";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fa_apply_info_ext");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp-t4:9091");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest
        return props;
    }

    private static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final KStream<String, FaApplyInfoExtOut> ksFo = source
                .map((k, v) -> {
                    KafkaData<FaApplyInfoExtSource> sdata = JSON.parseObject(v,
                            new TypeReference<KafkaData<FaApplyInfoExtSource>>() {
                            });
                    return new KeyValue<String, KafkaData<FaApplyInfoExtSource>>(k, sdata);
                })
                .filter((k, v) -> v != null && v.getTable().toLowerCase().equals("fa_apply_info_ext") &&
                        v.getData() != null && v.getData().size() > 0)
                .flatMap((k, v) -> {
                    List<KeyValue<String, FaApplyInfoExtOut>> keyValues = new ArrayList<>();
                    String vType = v.getType();
                    for (FaApplyInfoExtSource fs : v.getData()) {
                        FaApplyInfoExtOut fo = getFaApplyInfoExtOut(fs);
                        fo.setDbDMLType(vType);
                        keyValues.add(new KeyValue<String, FaApplyInfoExtOut>(fo.getID(), fo));
                    }
                    return keyValues;
                });
        ksFo.map((k, v) -> {
            return new KeyValue<String, String>(k, JSON.toJSONString(v));
        }).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
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

    private static FaApplyInfoExtOut getFaApplyInfoExtOut(FaApplyInfoExtSource f) {
        FaApplyInfoExtOut o = new FaApplyInfoExtOut();
        o.setDbDMLType(f.getDbDMLType());
        o.setID(f.getID());
        o.setApply_id(f.getApply_id());
        o.setRepaied_nper(f.getRepaied_nper());
        o.setRemain_period(f.getRemain_period());
        o.setResidual_principal(f.getResidual_principal());
        o.setOverdue_level(f.getOverdue_level());
        o.setOverdue_days(f.getOverdue_days());
        o.setOverdue_nper(f.getOverdue_nper());
        o.setRepay_date(f.getRepay_date());
        o.setRepay_status(f.getRepay_status());

        if (f.getRepay_status() != null) {
            switch (f.getRepay_status()) {
                case "01":
                    o.setRepay_status_name("正常还款");
                    break;
                case "02":
                    o.setRepay_status_name("逾期");
                    break;
                case "03":
                    o.setRepay_status_name("结清");
                    break;
                default:
                    o.setRepay_status_name("");
                    break;
            }
        }

        if (f.getRepay_status() != null) {
            if (f.getRepay_status().equals("02")) {
                o.setIs_overdue("1");
            } else {
                o.setIs_overdue("0");
            }
        }

        o.setIs_overdue(f.getSettle_status() == null ? "" : f.getSettle_status());

        if (f.getSettle_status() != null) {
            switch (f.getSettle_status()) {
                case "01":
                    o.setClear_type_name("正常结清");
                    break;
                case "02":
                    o.setClear_type_name("提前还款结清");
                    break;

                case "05":
                    o.setClear_type_name("买断结清");
                    break;
                case "06":
                    o.setClear_type_name("车主融强制结清");
                    break;
                case "07":
                    o.setClear_type_name("提前还车结清");
                    break;
                case "08":
                    o.setClear_type_name("展期结清");
                    break;
                case "09":
                    o.setClear_type_name("强制结清");
                    break;
                case "10":
                    o.setClear_type_name("展期结清（保有客户）");
                    break;
                case "11":
                    o.setClear_type_name("续租结清(保有客户)");
                    break;
                case "99":
                    o.setClear_type_name("合同取消");
                    break;
                case "21":
                    o.setClear_type_name("资源车到期结清");
                    break;
                case "22":
                    o.setClear_type_name("不良资产转让");
                    break;
                case "23":
                    o.setClear_type_name("全额债权转让");
                    break;
                case "24":
                    o.setClear_type_name("核销");
                    break;
                default:
                    o.setClear_type_name("其他");
                    break;

            }
        } else {
            o.setClear_type_name("其他");
        }

        o.setClear_time(f.getClean_date());
        o.setIs_repurchased(f.getRepurchased());
        o.setSale_overdue_amt(f.getLast_overdue_amount());
        o.setAlrepaid_amt(f.getTotal_repay_amount());
        o.setOverdue_month_cnt(f.getLast_overdue_nper());
        o.setIS_DELETED(f.getIS_DELETED());

        return o;
    }
}
