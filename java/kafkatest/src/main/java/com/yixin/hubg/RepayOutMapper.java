package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.OrderIn;
import com.yixin.hubg.domain.OrderOut;
import com.yixin.hubg.domain.RepayPlanCustOut;
import com.yixin.hubg.domain.RepayPlanCustTemp;
import com.yixin.hubg.util.DateTimeHelper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class RepayOutMapper implements KeyValueMapper<String, String, KeyValue<String, String>> {
    @Override
    public KeyValue<String, String> apply(String key, String value) {
        Map<Integer, RepayPlanCustTemp> rmap = JSON.parseObject(value
                , new TypeReference<Map<Integer, RepayPlanCustTemp>>() {
                });
        RepayPlanCustOut ret = new RepayPlanCustOut("", "", new BigDecimal(0), ""
                , "", "", "", "", ""
                , "", new BigDecimal(0), new BigDecimal(0), new BigDecimal(0), new BigDecimal(0)
                , new BigDecimal(0), new BigDecimal(0), 0);
        ret.setApply_no(key);
        BigDecimal first_month_pay = new BigDecimal(0);
        String first_plan_repay_date = "";
        String last_plan_repay_date = "";
        String first_overdue_date = "";
        String first_overdue_sernum = "";
        String cur_sernum = "";
        String cur_plan_repay_date = "";
        String last_withd_time = "";
        BigDecimal alrepaid_prlint = new BigDecimal(0);
        BigDecimal alrepaid_prin = new BigDecimal(0);
        BigDecimal alrepaid_intes = new BigDecimal(0);
        BigDecimal sale_ovpl_prlint = new BigDecimal(0);
        BigDecimal sale_ovpl_intes = new BigDecimal(0);
        BigDecimal sale_overdue_prin = new BigDecimal(0);
        int overdue_period_cnt_his = 0;
        for (Map.Entry<Integer, RepayPlanCustTemp> entry : rmap.entrySet()) {
            RepayPlanCustTemp rt = entry.getValue();
            ret.setID(rt.getID());

            //first_month_pay
            if (rt.getCurrent_repay_period().equals("1") && rt.getPlan_repay_total_money() != null
                    && !rt.getPlan_repay_total_money().equals("")
            ) {
                try {
                    first_month_pay = new BigDecimal(rt.getPlan_repay_total_money());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ret.setFirst_month_pay(first_month_pay);

            //first_plan_repay_date
            if (rt.getCurrent_repay_period().equals("1") && rt.getPlan_repay_date() != null) {
                first_plan_repay_date = rt.getPlan_repay_date();
            }
            ret.setFirst_plan_repay_date(first_plan_repay_date);

            //last_plan_repay_date
            if (rt.getPlan_repay_date() != null && rt.getPlan_repay_date().compareTo(last_plan_repay_date) > 0) {
                last_plan_repay_date = rt.getPlan_repay_date();
            }
            ret.setLast_plan_repay_date(last_plan_repay_date);

            //first_overdue_date first_overdue_sernum
            if (rt.getIs_overdue_his() != null && rt.getIs_overdue_his() == 1 && rt.getPlan_repay_date() != null) {
                if (ret.getFirst_overdue_date().equals("")) {
                    ret.setFirst_overdue_date(rt.getPlan_repay_date());
                    first_overdue_date = rt.getPlan_repay_date();
                    if (rt.getCurrent_repay_period() != null) {
                        ret.setFirst_overdue_sernum(rt.getCurrent_repay_period());
                        first_overdue_sernum = rt.getCurrent_repay_period();
                    }
                }
                if (rt.getPlan_repay_date().compareTo(ret.getFirst_overdue_date()) <= 0) {
                    first_overdue_date = rt.getPlan_repay_date();
                    first_overdue_sernum = rt.getCurrent_repay_period();
                }
            }
            ret.setFirst_overdue_date(first_overdue_date);
            ret.setFirst_overdue_sernum(first_overdue_sernum);

            //cur_sernum
            if (rt.getPlan_repay_date() != null
                    && rt.getPlan_repay_date().compareTo(DateTimeHelper.TimeStamp2String(new Date().getTime())) < 0
                    && rt.getCurrent_repay_period() != null
                    && !rt.getCurrent_repay_period().equals("99")
                    && rt.getCurrent_repay_period().compareTo(ret.getCur_sernum()) > 0
            ) {
                cur_sernum = rt.getCurrent_repay_period();
            }
            ret.setCur_sernum(cur_sernum);

            //cur_plan_repay_date
            if (rt.getPlan_repay_date() != null
                    && rt.getPlan_repay_date().compareTo(DateTimeHelper.TimeStamp2String(new Date().getTime())) < 0
                    && rt.getCurrent_repay_period() != null
                    && rt.getCurrent_repay_period() != null
                    && !rt.getCurrent_repay_period().equals("99")
                    && rt.getPlan_repay_date().compareTo(ret.getCur_plan_repay_date()) > 0
            ) {
                cur_plan_repay_date = rt.getPlan_repay_date();
            }
            ret.setCur_plan_repay_date(cur_plan_repay_date);

            //last_withd_time
            if (rt.getActual_repay_date() != null && rt.getActual_repay_date().compareTo(ret.getLast_withd_time()) > 0) {
                last_withd_time = rt.getActual_repay_date();
            }
            ret.setLast_withd_time(last_withd_time);

            //alrepaid_prlint
//            System.out.println("rt.getIs_alrepaid():" + rt.getIs_alrepaid());
//            System.out.println("rt.getPlan_repay_total_money():" + rt.getPlan_repay_total_money());
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_total_money() != null
                    && !rt.getPlan_repay_total_money().equals("")
                    && rt.getIs_alrepaid() == 1
            ) {
                try {
                    alrepaid_prlint = ret.getAlrepaid_prlint().add(new BigDecimal(rt.getPlan_repay_total_money()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                System.out.println("alrepaid_prlint:" + alrepaid_prlint);
            }
            ret.setAlrepaid_prlint(alrepaid_prlint);

            //alrepaid_prin
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_principal() != null
                    && !rt.getPlan_repay_principal().equals("")
                    && rt.getIs_alrepaid() == 1
            ) {
                try {
                    alrepaid_prin = ret.getAlrepaid_prin().add(new BigDecimal(rt.getPlan_repay_principal()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ret.setAlrepaid_prin(alrepaid_prin);

            //alrepaid_intes
//            System.out.println("rt.getIs_alrepaid():" + rt.getIs_alrepaid());
//            System.out.println("rt.getPlan_repay_interest():" + rt.getPlan_repay_total_money());
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_interest() != null
                    && !rt.getPlan_repay_interest().equals("")
                    && rt.getIs_alrepaid() == 1
            ) {
                try {
                    alrepaid_intes = ret.getAlrepaid_intes().add(new BigDecimal(rt.getPlan_repay_interest()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                System.out.println("alrepaid_intes:" + alrepaid_intes);
            }
            ret.setAlrepaid_intes(alrepaid_intes);

            //sale_ovpl_prlint 剩余本息_销售
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_total_money() != null
                    && !rt.getPlan_repay_total_money().equals("")
                    && rt.getIs_alrepaid() == 0
            ) {
                try {
                    sale_ovpl_prlint = ret.getSale_ovpl_prlint().add(new BigDecimal(rt.getPlan_repay_total_money()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ret.setSale_ovpl_prlint(sale_ovpl_prlint);

            //sale_ovpl_intes 剩余利息_销售
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_interest() != null
                    && !rt.getPlan_repay_interest().equals("")
                    && rt.getIs_alrepaid() == 0
            ) {
                try {
                    sale_ovpl_intes = ret.getSale_ovpl_intes().add(new BigDecimal(rt.getPlan_repay_interest()));
                } catch (Exception e) {
                    System.out.println(key + "|" + entry.getKey() + "|sale_ovpl_intes转型失败！");
                }
            }
            ret.setSale_ovpl_intes(sale_ovpl_intes);

            //sale_overdue_prin 当前逾期本金（销售）
//            System.out.println("rt.getIs_overdue_cur():" + rt.getIs_alrepaid());
//            System.out.println("rt.getPlan_repay_interest():" + rt.getPlan_repay_principal());
            if (rt.getIs_alrepaid() != null && rt.getPlan_repay_principal() != null
                    && !rt.getPlan_repay_principal().equals("")
                    && rt.getIs_overdue_cur() == 1
            ) {
                try {
                    sale_overdue_prin = ret.getSale_overdue_prin().add(new BigDecimal(rt.getPlan_repay_principal()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                System.out.println("sale_overdue_prin:" + sale_overdue_prin);
            }
            ret.setSale_overdue_prin(sale_overdue_prin);

            //overdue_period_cnt_his
            if (rt.getIs_overdue_his() != null) {
                overdue_period_cnt_his = ret.getOverdue_period_cnt_his() + rt.getIs_overdue_his();
            }
            ret.setOverdue_period_cnt_his(overdue_period_cnt_his);
        }
        return new KeyValue<String, String>(key, JSON.toJSONString(ret));
    }
}
