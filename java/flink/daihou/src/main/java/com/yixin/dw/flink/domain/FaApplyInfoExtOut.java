package com.yixin.dw.flink.domain;

import lombok.Data;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
public class FaApplyInfoExtOut extends KafkaEntiesBase {

    private String ID;
    private String apply_id;
    private String repaied_nper;
    private String remain_period;
    private String residual_principal;
    private String overdue_level;
    private String overdue_days;
    private String overdue_nper;
    private String repay_date;
    private String repay_status;
    private String repay_status_name;
    private String is_overdue;
    private String clear_type_id;
    private String clear_type_name;
    private String clear_time;
    private String is_repurchased;
    private String sale_overdue_amt;
    private String alrepaid_amt;
    private String overdue_month_cnt;
    private String IS_DELETED;

}
