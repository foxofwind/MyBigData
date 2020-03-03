package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/11
 **/
public class MyKafkaSplitter implements FlatMapFunction<KafkaData<FaApplyInfoExtSource>, FaApplyInfoExtSource> {
    @Override
    public void flatMap(KafkaData<FaApplyInfoExtSource> sources, Collector<FaApplyInfoExtSource> collector) throws Exception {
        if (sources != null && sources.getData() != null && sources.getData().size() > 0) {
            {
                for (FaApplyInfoExtSource item : sources.getData()) {
                    item.setDbDMLType(sources.getType());
                    collector.collect(item);
                }
            }
        }
    }
}
