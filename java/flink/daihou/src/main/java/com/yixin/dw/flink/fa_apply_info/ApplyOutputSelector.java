package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/19
 **/
public class ApplyOutputSelector implements OutputSelector<KafkaData<FaApplyInfoExtSource>> {
    @Override
    public Iterable<String> select(KafkaData<FaApplyInfoExtSource> value) {
        List<String> output = new ArrayList<>();
        if (value.getType().equals(OrderLevelDemo.insertType) && !value.getData().getIS_DELETED().equals("0")) {
            output.add(OrderLevelDemo.insertFlag);
        } else if (value.getType().equals(OrderLevelDemo.updateType)) {
            if (!value.getOld().getIS_DELETED().equals("0") && value.getData().getIS_DELETED().equals("0")) {
                //如果原来is_delete不等于0，现在等于0，相当于是新增的
                output.add(OrderLevelDemo.insertFlag);
            } else if (value.getOld().getIS_DELETED().equals("0") && !value.getData().getIS_DELETED().equals("0")) {
                //原来is_delete等于0，现在不等于0，相当于删除了
                output.add(OrderLevelDemo.deleteFlag);
            } else if (value.getOld().getIS_DELETED().equals("0") && value.getData().getIS_DELETED().equals("0")) {
                //前后都是0，更新
                output.add(OrderLevelDemo.updateFlag);
            } else {
                output.add(OrderLevelDemo.otherFlag);
            }

            output.add(OrderLevelDemo.updateFlag);
        } else if (value.getType().equals(OrderLevelDemo.deleteType)) {
            output.add(OrderLevelDemo.deleteFlag);
        } else {
            output.add(OrderLevelDemo.otherFlag);
        }
        return output;
    }
}
