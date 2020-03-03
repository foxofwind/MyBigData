package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.comm.MyKafkaDeserializationScheme;
import com.yixin.dw.flink.comm.MyKafkaSplitter;
import com.yixin.dw.flink.comm.MyPeriodicTsAndWmarks;
import com.yixin.dw.flink.domain.FaApplyInfoExtOut;
import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
public class OrderLevelDemo {
    static final String insertType="INSERT";
    static final String updateType="UPDATE";
    static final String deleteType="DELETE";

   static final String insertFlag = "insert";
    static final String updateFlag = "update";
    static final String deleteFlag = "delete";
    static final String otherFlag = "other";

        public static void main(String[] args) throws Exception {
        String ips = "10.0.4.98:9092,10.0.4.99:9092,10.0.4.100:9092";
        String topic = "yx_mysql_lloan";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", ips);
        props.setProperty("group.id", "group.cyb.1");
        FlinkKafkaConsumer010<KafkaData<List<FaApplyInfoExtSource>>> consumer =
                new FlinkKafkaConsumer010<>(topic, new MyKafkaDeserializationScheme<List<FaApplyInfoExtSource>>(), props);
        AssignerWithPeriodicWatermarks<KafkaData<List<FaApplyInfoExtSource>>> watermarks = new MyPeriodicTsAndWmarks();
        consumer.assignTimestampsAndWatermarks(watermarks);
        consumer.setStartFromEarliest();

        SplitStream<KafkaData<FaApplyInfoExtSource>> sourceDs = env
                .addSource(consumer)
                .filter(f -> f.getTable().toLowerCase().equals("fa_apply_info_ext"))
                .flatMap(new MyKafkaSplitter<FaApplyInfoExtSource>())
                .returns(new TypeHint<KafkaData<FaApplyInfoExtSource>>() {
                })
                .split(new ApplyOutputSelector())
                ;

        //insert
        sourceDs.select(insertFlag)
                .map(f->getFaApplyInfoExtOut(f))
                .returns(new TypeHint<FaApplyInfoExtOut>() {
                })
                .writeUsingOutputFormat(new HBaseOutputFormatInUpdate());

        //update
        sourceDs.select(updateFlag)
                .map(f->getFaApplyInfoExtOut(f))
                .returns(new TypeHint<FaApplyInfoExtOut>() {
                })
                .writeUsingOutputFormat(new HBaseOutputFormatInUpdate());

        //delete的要在sink目标里删除
        sourceDs.select(deleteFlag)
                .map(f->getFaApplyInfoExtOut(f))
                .returns(new TypeHint<FaApplyInfoExtOut>() {
                })
                .writeUsingOutputFormat(new HBaseOutputFormatDelete());

        env.execute("Kafka-Flink-dh-demo");
    }

    private static FaApplyInfoExtOut getFaApplyInfoExtOut(KafkaData<FaApplyInfoExtSource> f) {
        FaApplyInfoExtOut o = new FaApplyInfoExtOut();
        o.setType(f.getType());
        o.setID(f.getData().getID());
        o.setApply_id(f.getData().getApply_id());
        o.setRepaied_nper(f.getData().getRepaied_nper());
        o.setRemain_period(f.getData().getRemain_period());
        o.setResidual_principal(f.getData().getResidual_principal());
        o.setOverdue_level(f.getData().getOverdue_level());
        o.setOverdue_days(f.getData().getOverdue_days());
        o.setOverdue_nper(f.getData().getOverdue_nper());
        o.setRepay_date(f.getData().getRepay_date());
        o.setRepay_status(f.getData().getRepay_status());

        if (f.getData().getRepay_status() != null) {
            switch (f.getData().getRepay_status()) {
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

        if (f.getData().getRepay_status() != null) {
            if (f.getData().getRepay_status().equals("02")) {
                o.setIs_overdue("1");
            } else {
                o.setIs_overdue("0");
            }
        }

        o.setIs_overdue(f.getData().getSettle_status() == null ? "" : f.getData().getSettle_status());

        if (f.getData().getSettle_status() != null) {
            switch (f.getData().getSettle_status()) {
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

        o.setClear_time(f.getData().getClean_date());
        o.setIs_repurchased(f.getData().getRepurchased());
        o.setSale_overdue_amt(f.getData().getLast_overdue_amount());
        o.setAlrepaid_amt(f.getData().getTotal_repay_amount());
        o.setOverdue_month_cnt(f.getData().getLast_overdue_nper());
        o.setIS_DELETED(f.getData().getIS_DELETED());

        return o;
    }
}
