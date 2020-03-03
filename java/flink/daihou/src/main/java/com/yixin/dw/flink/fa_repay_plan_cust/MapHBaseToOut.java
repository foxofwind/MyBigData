package com.yixin.dw.flink.fa_repay_plan_cust;

import com.yixin.dw.flink.domain.*;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

/**
 * Create By 鸣宇淳 on 2020/2/21
 **/
public class MapHBaseToOut implements MapFunction<RepayPlanHBaseData, RepayPlanCustOut> {
    @Override
    public RepayPlanCustOut map(RepayPlanHBaseData hBaseData) throws Exception {
        RepayPlanCustOut out = new RepayPlanCustOut();

        Optional<BigDecimal> findDecimal=null;
        Optional<String> findString=null;
        Optional<BigDecimal> findInteger=null;

        out.setApply_no(hBaseData.getApply_no());

        findDecimal=  hBaseData.getFirst_month_pay().values().stream().reduce((s,v)->s=s.add(v));
        findDecimal.ifPresent(out::setFirst_month_pay);

        findString=hBaseData.getFirst_plan_repay_date().values().stream().min(String::compareTo);
        findString.ifPresent(out::setFirst_plan_repay_date);

        findString=hBaseData.getLast_plan_repay_date().values().stream().max(String::compareTo);
        findString.ifPresent(out::setLast_plan_repay_date);

        findString=hBaseData.getFirst_overdue_date().values().stream().min(String::compareTo);
        findString.ifPresent(out::setFirst_overdue_date);

        findString=hBaseData.getFirst_overdue_sernum().values().stream().min(String::compareTo);
        findString.ifPresent(out::setFirst_overdue_sernum);


//        private String first_overdue_sernum;
        findString=hBaseData.getFirst_overdue_sernum().values().stream().max(String::compareTo);
        findString.ifPresent(out::setFirst_overdue_sernum);

//        private String cur_sernum;
//        private String cur_plan_repay_date;
//        private String last_withd_time;
//        private BigDecimal alrepaid_prlint;
//        private BigDecimal alrepaid_prin;
//        private BigDecimal alrepaid_intes;
//        private BigDecimal sale_ovpl_prlint;
//        private BigDecimal sale_ovpl_intes;
//        private BigDecimal sale_overdue_prin;
//        private Integer overdue_period_cnt_his;


        return out;
    }
}
