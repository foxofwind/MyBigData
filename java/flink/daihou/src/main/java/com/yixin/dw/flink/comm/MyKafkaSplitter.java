package com.yixin.dw.flink.comm;

import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/11
 **/
public class MyKafkaSplitter<T>
        implements FlatMapFunction<KafkaData<List<T>>, KafkaData<T>> {
    @Override
    public void flatMap(KafkaData<List<T>> sources, Collector<KafkaData<T>> collector) {
        if (sources != null && sources.getData() != null && sources.getData().size() > 0) {
            {
                for (int i = 0; i < sources.getData().size(); i++) {
                    KafkaData<T> kafkaEnties = new KafkaData<T>();

                    kafkaEnties.setDatabase(sources.getDatabase());
                    kafkaEnties.setId(sources.getId());
                    kafkaEnties.setTable(sources.getTable());
                    kafkaEnties.setTs(sources.getTs());
                    kafkaEnties.setType(sources.getType());

                    kafkaEnties.setData(sources.getData().get(i));
                    kafkaEnties.setOld(sources.getOld().get(i));

                    collector.collect(kafkaEnties);
                }
            }
        }
    }
}
