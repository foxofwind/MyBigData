package com.yixin.dw.flink.fa_repay_plan_cust;

import com.yixin.dw.flink.domain.RepayPlanCustSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/19
 **/
public class CustOutputSelector implements OutputSelector<KafkaData<RepayPlanCustSource>> {
    @Override
    public Iterable<String> select(KafkaData<RepayPlanCustSource> value) {
        List<String> output = new ArrayList<>();
        if (value.getType().equals(OrderLevel2Demo.insertType)) {
            if (value.getData().getIS_DELETED().equals("0") && !value.getData().getCurrent_repay_period().equals("0")) {
                //如果是insert，如果符合条件（符合where条件），就是正向计算
                output.add(OrderLevel2Demo.addFlag);
            } else {
                // 如果不符合条件，不用处理
                output.add(OrderLevel2Demo.otherFlag);
            }
        } else if (value.getType().equals(OrderLevel2Demo.updateType)) {
            if (value.getOld().getIS_DELETED().equals("0")
                    && !value.getOld().getCurrent_repay_period().equals("0")
                    && value.getData().getIS_DELETED().equals("0")
                    && !value.getData().getCurrent_repay_period().equals("0")
            ) {
                //如果是update，如果原来符合条件，现在还符合条件，正向处理
                output.add(OrderLevel2Demo.addFlag);
            } else if (value.getOld().getIS_DELETED().equals("0")
                    && !value.getOld().getCurrent_repay_period().equals("0")
                    && (!value.getData().getIS_DELETED().equals("0")
                    || value.getData().getCurrent_repay_period().equals("0"))
            ) {
                //如果原来符合条件，现在不符合条件，反向处理
                output.add(OrderLevel2Demo.subFlag);
            }
            else if ((!value.getOld().getIS_DELETED().equals("0")
                    || value.getOld().getCurrent_repay_period().equals("0"))
                    && value.getData().getIS_DELETED().equals("0")
                    && !value.getData().getCurrent_repay_period().equals("0")
            ) {
                //如果原来不符合条件，现在符合条件，正向处理
                output.add(OrderLevel2Demo.addFlag);
            }
            else
            {
                // 如果原来不符合条件，现在还不符合条件，不用处理
                output.add(OrderLevel2Demo.otherFlag);
            }

        } else if (value.getType().equals(OrderLevel2Demo.deleteType)) {
            if (value.getOld().getIS_DELETED().equals("0")
                    && !value.getOld().getCurrent_repay_period().equals("0")
            ) {
                //如果是delete，如果原来符合条件，反向处理
                output.add(OrderLevel2Demo.subFlag);
            }
            else
            {
                output.add(OrderLevel2Demo.otherFlag);
            }
        } else {
            output.add(OrderLevel2Demo.otherFlag);
        }
        return output;
    }
}
