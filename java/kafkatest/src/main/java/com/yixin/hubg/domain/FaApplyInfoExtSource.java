package com.yixin.hubg.domain;

import lombok.Data;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
public class FaApplyInfoExtSource {
    private String dbDMLType;//操作类型
    private String ID;
    private String BZ_ID;
    private String CREATE_TIME;
    private String CREATOR_DEPARTMENT_ID;
    private String CREATOR_DEPARTMENT_NAME;
    private String CREATOR_ID;
    private String CREATOR_NAME;
    private String IS_DELETED;
    private String UPDATE_TIME;
    private String UPDATOR_DEPARTMENT_ID;
    private String UPDATOR_DEPARTMENT_NAME;
    private String UPDATOR_ID;
    private String UPDATOR_NAME;
    private String VERSION;
    private String apply_id;
    private String clean_date;
    private String frist_overdue_time;
    private String is_overdue;
    private String last_overdue_amount;
    private String last_overdue_days;
    private String last_overdue_nper;
    private String loan_amount;
    private String max_overdue_days;
    private String overdue_count;
    private String overdue_days;
    private String overdue_nper;
    private String overdue_principal_interest;
    private String remain_period;
    private String repaied_nper;
    private String repay_date;
    private String repay_status;
    private String residual_principal;
    private String settle_status;
    private String term;
    private String total_change_amt;
    private String total_overdue_amount;
    private String total_overdue_days;
    private String total_repay_amount;
    private String total_exempt_debt;
    private String total_overdue_penalty;
    private String data_source;
    private String repurchased;
    private String overdue_level;
    private String concel_flag;
    private String concel_resion;
    private String over_3days_count;
    private String over_M2_count;
    private String month_pay;
    private String residual_instrest;
    private String next_repay_date;
    private String current_overdue_principal;
    private String current_overdue_instrest;
    private String max_overdue_term;
    private String max_overdue_principal;
    private String settle_clssify;
    private String bank_repay_status;
    private String bank_residual_principal;
    private String bank_residual_instrest;
    private String residual_principal_interest;
    private String total_principal_interest;
    private String after_settle_back_amt;
    private String before_settle_back_amt;
    private String rent_out_type;
    private String is_all_pay;
    private String is_penalty_plan;
    private String repaied_principal;
    private String repaied_instrest;
    private String continue_overdue_nper;
    private String continue_normal_pay_nper;
    private String insurance_out_flag;
    private String is_daic;
    private String last_normal_pay_nper;
}
