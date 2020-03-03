package com.yixin.dw.flink.fa_repay_plan_cust;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.yixin.dw.flink.domain.*;
import com.yixin.dw.flink.util.HBaseHelper;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * Create By 鸣宇淳 on 2020/2/20
 **/
public class CustAllWindowFunction
        implements AllWindowFunction<DataContainer<RepayPlanCustOut>, RepayPlanHBaseData, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<DataContainer<RepayPlanCustOut>> values,
                      Collector<RepayPlanHBaseData> out) throws Exception {
        List<DataContainer<RepayPlanCustOut>> list = new ArrayList<>();
        values.forEach(list::add);
        if (list.size() > 0) {
            RepayPlanHBaseData hBaseData = getDataFromHBase(list.get(0).getKey());
            for (DataContainer<RepayPlanCustOut> item : list) {
                RepayPlanCustOut newData = item.getData();
                String ID = newData.getID();
                if (item.getDbDMLType().equals(OrderLevel2Demo.insertType)
                        || item.getDbDMLType().equals(OrderLevel2Demo.updateType)
                ) {
                    //新增或者编辑记录
                    hBaseData.getFirst_month_pay().put(ID, newData.getFirst_month_pay());
                    hBaseData.getFirst_plan_repay_date().put(ID, newData.getFirst_plan_repay_date());
                    hBaseData.getLast_plan_repay_date().put(ID, newData.getLast_plan_repay_date());
                    hBaseData.getFirst_overdue_date().put(ID, newData.getFirst_overdue_date());
                    hBaseData.getFirst_overdue_sernum().put(ID, newData.getFirst_overdue_sernum());
                    hBaseData.getCur_sernum().put(ID, newData.getCur_sernum());
                    hBaseData.getCur_plan_repay_date().put(ID, newData.getCur_plan_repay_date());
                    hBaseData.getLast_withd_time().put(ID, newData.getLast_withd_time());
                    hBaseData.getAlrepaid_prlint().put(ID, newData.getAlrepaid_prlint());
                    hBaseData.getAlrepaid_prin().put(ID, newData.getAlrepaid_prin());
                    hBaseData.getAlrepaid_intes().put(ID, newData.getAlrepaid_intes());
                    hBaseData.getSale_ovpl_prlint().put(ID, newData.getSale_ovpl_prlint());
                    hBaseData.getSale_ovpl_intes().put(ID, newData.getSale_ovpl_intes());
                    hBaseData.getSale_overdue_prin().put(ID, newData.getSale_overdue_prin());
                    hBaseData.getOverdue_period_cnt_his().put(ID, newData.getOverdue_period_cnt_his());
                } else if (item.getDbDMLType().equals(OrderLevel2Demo.deleteType)) {
                    //删除这一行记录
                    hBaseData.getFirst_month_pay().remove(ID);
                    hBaseData.getFirst_plan_repay_date().remove(ID);
                    hBaseData.getLast_plan_repay_date().remove(ID);
                    hBaseData.getFirst_overdue_date().remove(ID);
                    hBaseData.getFirst_overdue_sernum().remove(ID);
                    hBaseData.getCur_sernum().remove(ID);
                    hBaseData.getCur_plan_repay_date().remove(ID);
                    hBaseData.getLast_withd_time().remove(ID);
                    hBaseData.getAlrepaid_prlint().remove(ID);
                    hBaseData.getAlrepaid_prin().remove(ID);
                    hBaseData.getAlrepaid_intes().remove(ID);
                    hBaseData.getSale_ovpl_prlint().remove(ID);
                    hBaseData.getSale_ovpl_intes().remove(ID);
                    hBaseData.getSale_overdue_prin().remove(ID);
                    hBaseData.getOverdue_period_cnt_his().remove(ID);
                }
                out.collect(hBaseData);
            }
        }
    }

    public RepayPlanHBaseData getDataFromHBase(String applyNo) throws IOException {
        HBaseHelper hBaseHelper = new HBaseHelper(OrderLevel2Demo.hbaseTableName);
        Result result = hBaseHelper.getByRowKey(OrderLevel2Demo.hbaseTableName, applyNo);
        RepayPlanHBaseData hBaseData = new RepayPlanHBaseData();
        hBaseData.setApply_no(applyNo);
        if (!result.isEmpty()) {
            hBaseData = new RepayPlanHBaseData();

            String rowKey = Bytes.toString(result.getRow());
            hBaseData.setApply_no(rowKey);

            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                switch (qualifier) {
                    case "first_month_pay":
                        hBaseData.setFirst_month_pay(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "first_plan_repay_date":
                        hBaseData.setFirst_plan_repay_date(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;

                    case "last_plan_repay_date":
                        hBaseData.setLast_plan_repay_date(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "first_overdue_date":
                        hBaseData.setFirst_overdue_date(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "first_overdue_sernum":
                        hBaseData.setFirst_overdue_sernum(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "cur_sernum":
                        hBaseData.setCur_sernum(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "cur_plan_repay_date":
                        hBaseData.setCur_plan_repay_date(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "last_withd_time":
                        hBaseData.setLast_withd_time(JSON.parseObject(val, new TypeReference<HashMap<String, String>>() {
                        }));
                        break;
                    case "alrepaid_prlint":
                        hBaseData.setAlrepaid_prlint(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "alrepaid_prin":
                        hBaseData.setAlrepaid_prin(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "alrepaid_intes":
                        hBaseData.setAlrepaid_intes(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "sale_ovpl_prlint":
                        hBaseData.setSale_ovpl_prlint(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "sale_ovpl_intes":
                        hBaseData.setSale_ovpl_intes(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "sale_overdue_prin":
                        hBaseData.setSale_overdue_prin(JSON.parseObject(val, new TypeReference<HashMap<String, BigDecimal>>() {
                        }));
                        break;
                    case "overdue_period_cnt_his":
                        hBaseData.setOverdue_period_cnt_his(JSON.parseObject(val, new TypeReference<HashMap<String, Integer>>() {
                        }));
                        break;
                }
            }
        }
        return hBaseData;
    }

}
