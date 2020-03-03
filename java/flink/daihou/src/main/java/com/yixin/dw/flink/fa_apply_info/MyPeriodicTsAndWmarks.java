package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Create By 鸣宇淳 on 2020/2/12
 **/
public class MyPeriodicTsAndWmarks implements AssignerWithPeriodicWatermarks<KafkaData<FaApplyInfoExtSource>> {
    private final long maxTimeLag = 5000; // 5 seconds

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

    @Override
    public long extractTimestamp(KafkaData<FaApplyInfoExtSource> element, long previousElementTimestamp) {
        return element.getTs();
    }
}
