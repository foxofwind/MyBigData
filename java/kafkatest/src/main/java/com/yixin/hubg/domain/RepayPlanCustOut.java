package com.yixin.hubg.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Create By 鸣宇淳 on 2020/2/18
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RepayPlanCustOut {
    private String apply_no;
    private String ID;
    private BigDecimal first_month_pay;
    private String first_plan_repay_date;
    private String last_plan_repay_date;
    private String first_overdue_date;
    private String first_overdue_sernum;
    private String cur_sernum;
    private String cur_plan_repay_date;
    private String last_withd_time;
    private BigDecimal alrepaid_prlint;
    private BigDecimal alrepaid_prin;
    private BigDecimal alrepaid_intes;
    private BigDecimal sale_ovpl_prlint;
    private BigDecimal sale_ovpl_intes;
    private BigDecimal sale_overdue_prin;
    private Integer overdue_period_cnt_his;
}
