package com.yixin.hubg.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Create By 鸣宇淳 on 2020/2/17
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RepayPlanCustSource  extends KafkaEntiesBase{
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
    private String actual_interest_split;
    private String actual_repay_date;
    private String actual_repay_interest;
    private String actual_repay_principal;
    private String actual_repay_total_money;
    private String apply_no;
    private String balance;
    private String business_type;
    private String buyback_total_money;
    private String buyback_total_penalty;
    private String compensatory_money;
    private String current_repay_period;
    private String customer_account;
    private String due_penalty;
    private String early_repay_penalty;
    private String early_repay_process_money;
    private String early_repay_total_money;
    private String exempt_debt;
    private String guarantee_fee;
    private String loan_number;
    private String order_repayment_date;
    private String organization;
    private String overdue_days;
    private String overdue_penalty_date;
    private String overdue_penalty_money;
    private String overdue_repay_frequency;
    private String payment_channel_status;
    private String plan_repay_date;
    private String plan_repay_interest;
    private String plan_repay_principal;
    private String plan_repay_total_money;
    private String recovery_poundage;
    private String repay_state;
    private String service_charge;
    private String surplus_period;
    private String surplus_repay_interest;
    private String surplus_repay_principal;
    private String surplus_repay_total_money;
    private String total_repay_period;
    private String stage;
    private String charge_back_status;
    private String offset_margin_amt;
    private String is_locked;
    private String never_pay;
    private String zbkk;
    private String deduct_prepay_amt;
}
