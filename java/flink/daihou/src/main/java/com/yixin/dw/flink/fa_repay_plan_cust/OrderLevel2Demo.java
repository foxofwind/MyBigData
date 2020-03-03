package com.yixin.dw.flink.fa_repay_plan_cust;

import com.yixin.dw.flink.comm.MyKafkaDeserializationScheme;
import com.yixin.dw.flink.comm.MyKafkaSplitter;
import com.yixin.dw.flink.comm.MyPeriodicTsAndWmarks;
import com.yixin.dw.flink.domain.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Properties;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
public class OrderLevel2Demo {
    static final String insertType = "INSERT";
    static final String updateType = "UPDATE";
    static final String deleteType = "DELETE";

    static final String addFlag = "add";
    static final String subFlag = "sub";
    static final String otherFlag = "other";

    static final String mySqlTableName = "fa_repay_plan_cust";
    static final String hbaseTableName = "RT:testSinkCust";

    public static void main(String[] args) throws Exception {
        String ips = "10.0.4.98:9092,10.0.4.99:9092,10.0.4.100:9092";
        String topic = "yx_mysql_lloan";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Kafka设置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", ips);
        props.setProperty("group.id", "group.cyb.2");
        FlinkKafkaConsumer010<KafkaData<List<RepayPlanCustSource>>> consumer =
                new FlinkKafkaConsumer010<>(topic, new MyKafkaDeserializationScheme<List<RepayPlanCustSource>>(), props);

        //设置水位线
        AssignerWithPeriodicWatermarks<KafkaData<List<RepayPlanCustSource>>> watermarks = new MyPeriodicTsAndWmarks<>();
        consumer.assignTimestampsAndWatermarks(watermarks);
        consumer.setStartFromEarliest();

        DataStream<RepayPlanHBaseData> sourceDs = env
                .addSource(consumer)
                .filter(f -> f.getTable().toLowerCase().equals(mySqlTableName))
                .flatMap(new MyKafkaSplitter<RepayPlanCustSource>())
                .map(new MapSourceToOut())
                .keyBy("key")
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CustAllWindowFunction())
                .returns(new TypeHint<RepayPlanHBaseData>() {
                })
        ;
        //保存状态
        sourceDs.writeUsingOutputFormat(new CustHBaseStatusOutFormat());

        //计算结果，结果保存到HBase
        sourceDs.map(new MapHBaseToOut())
        .writeUsingOutputFormat(new CustHBaseSinkOutFormat());
        ;


        env.execute("Kafka-Flink-dh-demo2");
    }

}
