package com.yixin.dw.flink.fa_repay_plan_cust;

import com.yixin.dw.flink.domain.*;
import com.yixin.dw.flink.util.DateTimeHelper;
import com.yixin.dw.flink.util.HBaseHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.Result;
import scala.Int;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Create By 鸣宇淳 on 2020/2/20
 **/
public class MapSourceToOut implements MapFunction<KafkaData<RepayPlanCustSource>,DataContainer<RepayPlanCustOut>> {

    @Override
    public DataContainer<RepayPlanCustOut> map(KafkaData<RepayPlanCustSource> soure) throws Exception {
        DataContainer<RepayPlanCustTemp> kdTemp = new DataContainer<>();
        kdTemp.setData(getTempFromSource(soure.getData()));
        if (soure.getOld() != null) {
            kdTemp.setOld(getTempFromSource(soure.getOld()));
            //用新数据填充Old数据
            kdTemp = paddingOld(kdTemp);
        }
        //将Temp进一步转换为Out
        DataContainer<RepayPlanCustOut> kdOut = new DataContainer<>();
        kdOut.setDbDMLType(soure.getType());
        kdOut.setKey(soure.getData().getApply_no());
        kdOut.setData(getOutFromTemp(kdTemp.getData()));
        if (kdTemp.getOld() != null) {
            kdOut.setOld(getOutFromTemp(kdTemp.getOld()));
        }
        return kdOut;
    }


    private static RepayPlanCustOut getOutFromTemp(RepayPlanCustTemp temp) {

        RepayPlanCustOut out = new RepayPlanCustOut();
        out.setApply_no(temp.getApply_no());
        out.setID(temp.getID());

        //first_month_pay
        BigDecimal first_month_pay = new BigDecimal(0);
        if (temp.getCurrent_repay_period().equals("1") && temp.getPlan_repay_total_money() != null) {
            first_month_pay = new BigDecimal(temp.getPlan_repay_total_money());
        }
        out.setFirst_month_pay(first_month_pay);

        //first_plan_repay_date
        String first_plan_repay_date = "";
        if (temp.getCurrent_repay_period().equals("1") && temp.getPlan_repay_date() != null) {
            first_plan_repay_date = temp.getPlan_repay_date();
        }
        out.setFirst_plan_repay_date(first_plan_repay_date);

        //last_plan_repay_date
        out.setLast_plan_repay_date(temp.getPlan_repay_date());

        //first_overdue_date
        String first_overdue_date = "";
        if (temp.getIs_overdue_his().equals("1") && temp.getPlan_repay_date() != null) {
            first_overdue_date = temp.getPlan_repay_date();
        }
        out.setFirst_overdue_date(first_overdue_date);

        //first_overdue_sernum
        String first_overdue_sernum = "";
        if (temp.getIs_overdue_his().equals("1") && temp.getCurrent_repay_period() != null) {
            first_overdue_sernum = temp.getCurrent_repay_period();
        }
        out.setFirst_overdue_sernum(first_overdue_sernum);

        //cur_sernum
        String cur_sernum = "";
        if (temp.getPlan_repay_date().compareTo(DateTimeHelper.TimeStamp2String(new Date().getTime())) < 0
                && temp.getCurrent_repay_period() != null
                && !temp.getCurrent_repay_period().equals("99")
        ) {
            cur_sernum = temp.getCurrent_repay_period();
        }
        out.setCur_sernum(cur_sernum);

        //cur_plan_repay_date
        String cur_plan_repay_date = "";
        if (temp.getPlan_repay_date().compareTo(DateTimeHelper.TimeStamp2String(new Date().getTime())) < 0
                && temp.getCurrent_repay_period() != null
                && temp.getCurrent_repay_period() != null
                && !temp.getCurrent_repay_period().equals("99")
        ) {
            cur_plan_repay_date = temp.getPlan_repay_date();
        }
        out.setCur_plan_repay_date(cur_plan_repay_date);

        //last_withd_time
        String last_withd_time = "";
        if (temp.getActual_repay_date() != null) {
            last_withd_time = temp.getActual_repay_date();
        }
        out.setLast_withd_time(last_withd_time);

        //alrepaid_prlint
        BigDecimal alrepaid_prlint = new BigDecimal(0);
        if (temp.getIs_alrepaid() != null && temp.getPlan_repay_total_money() != null
                && temp.getIs_alrepaid().equals("1")
        ) {
            alrepaid_prlint = new BigDecimal(temp.getPlan_repay_total_money());
        }
        out.setAlrepaid_prlint(alrepaid_prlint);

        //alrepaid_prin
        BigDecimal alrepaid_prin = new BigDecimal(0);
        if (temp.getIs_alrepaid() != null && temp.getPlan_repay_principal() != null
                && temp.getIs_alrepaid().equals("1")
        ) {
            alrepaid_prin = new BigDecimal(temp.getPlan_repay_principal());
        }
        out.setAlrepaid_prin(alrepaid_prin);

        //alrepaid_intes
        BigDecimal alrepaid_intes = new BigDecimal(0);
        if (temp.getIs_alrepaid() != null && temp.getPlan_repay_interest() != null
                && temp.getIs_alrepaid().equals("1")
        ) {
            alrepaid_intes = new BigDecimal(temp.getPlan_repay_interest());
        }
        out.setAlrepaid_intes(alrepaid_intes);

        //sale_ovpl_prlint
        BigDecimal sale_ovpl_prlint = new BigDecimal(0);
        if (temp.getIs_alrepaid() != null && temp.getPlan_repay_total_money() != null
                && temp.getIs_alrepaid().equals("0")
        ) {
            sale_ovpl_prlint = new BigDecimal(temp.getPlan_repay_total_money());
        }
        out.setSale_ovpl_prlint(sale_ovpl_prlint);

        //sale_ovpl_prlint
        BigDecimal sale_ovpl_intes = new BigDecimal(0);
        if (temp.getIs_alrepaid() != null && temp.getPlan_repay_interest() != null
                && temp.getIs_alrepaid().equals("0")
        ) {
            sale_ovpl_intes = new BigDecimal(temp.getPlan_repay_interest());
        }
        out.setSale_ovpl_prlint(sale_ovpl_intes);

        //overdue_period_cnt_his
        Integer overdue_period_cnt_his = 0;
        if (temp.getIs_overdue_his() != null) {
            overdue_period_cnt_his = temp.getIs_overdue_his();
        }
        out.setOverdue_period_cnt_his(overdue_period_cnt_his);

        return out;
    }

    private static RepayPlanCustTemp getTempFromSource(RepayPlanCustSource f) {
        RepayPlanCustTemp o = new RepayPlanCustTemp();
        o.setDbDMLType(f.getType());
        if (f.getApply_no() != null) {
            o.setApply_no(f.getApply_no());
        }
        if(f.getID()!=null)
        {
            o.setID(f.getID());
        }

        if (f.getPlan_repay_date() != null) {
            if (f.getPlan_repay_date().length() == 8) {
                String planRepay_date = f.getPlan_repay_date().substring(0, 4)
                        + "-" + f.getPlan_repay_date().substring(4, 2)
                        + "-" + f.getPlan_repay_date().substring(6, 2);
                o.setPlan_repay_date(planRepay_date);
            } else {
                o.setPlan_repay_date(f.getPlan_repay_date());
            }
        }

        if (f.getCurrent_repay_period() != null) {
            o.setCurrent_repay_period(f.getCurrent_repay_period());
        }

        if (f.getActual_repay_date() != null) {
            if (f.getActual_repay_date().length() == 8) {
                String actual_repay_date = f.getActual_repay_date().substring(0, 4)
                        + "-" + f.getActual_repay_date().substring(4, 2)
                        + "-" + f.getActual_repay_date().substring(6, 2)
                        + " 00:00:00";
                o.setActual_repay_date(actual_repay_date);
            } else if (f.getActual_repay_date().length() == 10) {
                o.setActual_repay_date(f.getActual_repay_date() + " 00:00:00");
            } else {
                o.setActual_repay_date(f.getActual_repay_date());
            }
        }

        if (f.getPlan_repay_principal() != null) {
            o.setPlan_repay_principal(f.getPlan_repay_principal());
        }

        if (f.getPlan_repay_interest() != null) {
            o.setPlan_repay_interest(f.getPlan_repay_interest());
        }

        if (f.getOverdue_penalty_money() != null) {
            o.setOverdue_penalty_money(f.getOverdue_penalty_money());
        }

        if (f.getRepay_state() != null) {
            o.setRepay_state(f.getRepay_state());
        }

        if (f.getActual_repay_total_money() != null) {
            o.setActual_repay_total_money(f.getActual_repay_total_money());
        }
        if (f.getOverdue_days() != null) {
            o.setOverdue_days(f.getOverdue_days());
        }

        if (f.getOverdue_days() != null) {
            if (f.getOverdue_days().compareTo("0") > 0) {
                o.setIs_overdue_his(1);
            } else {
                o.setIs_overdue_his(0);
            }
        }

        if (f.getRepay_state() != null) {
            if (f.getRepay_state() != null) {
                if (f.getRepay_state().equals("2")
                        || f.getRepay_state().equals("5")
                        || f.getRepay_state().equals("16")
                        || f.getRepay_state().equals("20")
                        || f.getRepay_state().equals("8")
                        || f.getRepay_state().equals("3")
                        || f.getRepay_state().equals("19")

                ) {
                    o.setIs_alrepaid(1);
                } else {
                    o.setIs_alrepaid(0);
                }
            } else {
                o.setIs_alrepaid(0);
            }


            if (f.getRepay_state() != null) {
                if (!f.getRepay_state().equals("2")
                        && !f.getRepay_state().equals("5")
                        && !f.getRepay_state().equals("16")
                        && !f.getRepay_state().equals("20")
                        && !f.getRepay_state().equals("8")
                        && !f.getRepay_state().equals("3")
                        && !f.getRepay_state().equals("19")
                        && !f.getRepay_state().equals("1")
                ) {
                    o.setIs_overdue_cur(1);
                } else {
                    o.setIs_overdue_cur(0);
                }
            } else {
                o.setIs_overdue_cur(0);
            }
        }
        if (f.getIS_DELETED() != null) {
            o.setIS_DELETED(f.getIS_DELETED());
        }
        return o;
    }

    public static DataContainer<RepayPlanCustTemp> paddingOld(DataContainer<RepayPlanCustTemp> tempKafkaData) {
        RepayPlanCustTemp oldData = tempKafkaData.getOld();
        RepayPlanCustTemp newData = tempKafkaData.getData();

        if (oldData.getActual_repay_date() == null) oldData.setActual_repay_date(newData.getActual_repay_date());
        if (oldData.getActual_repay_total_money() == null)
            oldData.setActual_repay_total_money(newData.getActual_repay_total_money());
        if (oldData.getApply_no() == null) oldData.setApply_no(newData.getApply_no());
        if (oldData.getCurrent_repay_period() == null)
            oldData.setCurrent_repay_period(newData.getCurrent_repay_period());
        if (oldData.getIs_alrepaid() == null) oldData.setIs_alrepaid(newData.getIs_alrepaid());
        if (oldData.getIS_DELETED() == null) oldData.setIS_DELETED(newData.getIS_DELETED());
        if (oldData.getIs_overdue_cur() == null) oldData.setIs_overdue_cur(newData.getIs_overdue_cur());
        if (oldData.getIs_overdue_his() == null) oldData.setIs_overdue_his(newData.getIs_overdue_his());
        if (oldData.getOverdue_days() == null) oldData.setOverdue_days(newData.getOverdue_days());
        if (oldData.getOverdue_penalty_money() == null)
            oldData.setOverdue_penalty_money(newData.getOverdue_penalty_money());
        if (oldData.getPlan_repay_date() == null) oldData.setPlan_repay_date(newData.getPlan_repay_date());
        if (oldData.getPlan_repay_interest() == null) oldData.setPlan_repay_interest(newData.getPlan_repay_interest());
        if (oldData.getPlan_repay_principal() == null)
            oldData.setPlan_repay_principal(newData.getPlan_repay_principal());
        if (oldData.getPlan_repay_total_money() == null)
            oldData.setPlan_repay_total_money(newData.getPlan_repay_total_money());
        if (oldData.getProcessType() == null) oldData.setProcessType(newData.getProcessType());
        if (oldData.getRepay_state() == null) oldData.setRepay_state(newData.getRepay_state());

        tempKafkaData.setOld(oldData);
        return tempKafkaData;
    }

}
