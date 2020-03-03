package com.yixin.dw.flink.domain;

import lombok.Data;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Create By 鸣宇淳 on 2020/2/20
 **/
@Data
public class RepayPlanHBaseData {

    public RepayPlanHBaseData()
    {
        this.apply_no="";
        this.first_month_pay=new HashMap<>();
        this.first_plan_repay_date=new HashMap<>();
        this.last_plan_repay_date=new HashMap<>();
        this.first_overdue_date=new HashMap<>();
        this.first_overdue_sernum=new HashMap<>();
        this.cur_sernum=new HashMap<>();
        this.cur_plan_repay_date=new HashMap<>();
        this.last_withd_time=new HashMap<>();
        this.alrepaid_prlint=new HashMap<>();
        this.alrepaid_prin=new HashMap<>();
        this.alrepaid_intes=new HashMap<>();
        this.sale_ovpl_prlint=new HashMap<>();
        this.sale_ovpl_intes=new HashMap<>();
        this.sale_overdue_prin=new HashMap<>();
        this.overdue_period_cnt_his=new HashMap<>();
    }
    private String apply_no;
    private HashMap<String,BigDecimal> first_month_pay;
    private HashMap<String,String> first_plan_repay_date;
    private HashMap<String,String> last_plan_repay_date;
    private HashMap<String,String> first_overdue_date;
    private HashMap<String,String> first_overdue_sernum;
    private HashMap<String,String> cur_sernum;
    private HashMap<String,String> cur_plan_repay_date;
    private HashMap<String,String> last_withd_time;
    private HashMap<String,BigDecimal> alrepaid_prlint;
    private HashMap<String,BigDecimal> alrepaid_prin;
    private HashMap<String,BigDecimal> alrepaid_intes;
    private HashMap<String,BigDecimal> sale_ovpl_prlint;
    private HashMap<String,BigDecimal> sale_ovpl_intes;
    private HashMap<String,BigDecimal> sale_overdue_prin;
    private HashMap<String,Integer> overdue_period_cnt_his;
}
