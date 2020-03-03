package com.yixin.dw.flink.fa_repay_plan_cust;

import com.yixin.dw.flink.domain.RepayPlanCustOut;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Create By 鸣宇淳 on 2020/2/20
 **/
public class CustProcessAllWindowFunction extends ProcessAllWindowFunction<RepayPlanCustOut, RepayPlanCustOut, TimeWindow> {
    @Override
    public void process(Context context, Iterable<RepayPlanCustOut> elements, Collector<RepayPlanCustOut> out) throws Exception {

    }
}
