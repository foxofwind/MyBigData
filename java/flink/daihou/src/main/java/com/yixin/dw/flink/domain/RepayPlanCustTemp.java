package com.yixin.dw.flink.domain;

import com.yixin.dw.flink.domain.KafkaEntiesBase;
import lombok.Data;

/**
 * Create By 鸣宇淳 on 2020/2/18
 **/
@Data
public class RepayPlanCustTemp {
    private Integer processType;//正向处理、反向处理
    private String dbDMLType;
    private String ID;
    private String apply_no;
    private String plan_repay_date;
    private String current_repay_period;
    private String actual_repay_date;
    private String plan_repay_principal;
    private String plan_repay_interest;
    private String overdue_penalty_money;
    private String repay_state;
    private String actual_repay_total_money;
    private String plan_repay_total_money;
    private String overdue_days;
    private Integer is_overdue_his;
    private Integer is_alrepaid;
    private Integer is_overdue_cur;
    private String IS_DELETED;
}
