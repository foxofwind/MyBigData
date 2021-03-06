package com.yixin.dw.flink.fa_repay_plan_cust;

import com.alibaba.fastjson.JSON;
import com.yixin.dw.flink.domain.RepayPlanCustOut;
import com.yixin.dw.flink.domain.RepayPlanHBaseData;
import com.yixin.dw.flink.util.StringHelper;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;


/**
 * Create By 鸣宇淳 on 2020/2/11
 *
 * create 'RT:testSinkCust',{NAME => 'cf', VERSIONS => 1,COMPRESSION=>'SNAPPY'}
 **/
public class CustHBaseStatusOutFormat implements OutputFormat<RepayPlanHBaseData> {
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;
    private final String hbaseTableName = "RT:testStatusCust";

    @Override
    public void configure(Configuration parameters) {


    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        conf = HBaseConfiguration.create();

        conf.set("zookeeper.znode.parent", "/hbase2-secure");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.0.4.91,10.0.4.90,10.0.4.92");
        conf.set("hbase.client.retries.number", "1");

        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(hbaseTableName));
    }

    @Override
    public void writeRecord(RepayPlanHBaseData record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.getApply_no()));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("apply_id"),
                Bytes.toBytes(StringHelper.toStringNotNull(record.getApply_no())));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("first_month_pay"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getFirst_month_pay()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("first_plan_repay_date"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getFirst_plan_repay_date()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_plan_repay_date"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getLast_plan_repay_date()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("first_overdue_date"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getFirst_overdue_date()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("first_overdue_sernum"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getFirst_overdue_sernum()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cur_sernum"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getCur_sernum()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cur_plan_repay_date"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getCur_plan_repay_date()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_withd_time"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getLast_withd_time()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("alrepaid_prlint"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getAlrepaid_prlint()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("alrepaid_prin"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getAlrepaid_prin()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("alrepaid_intes"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getAlrepaid_intes()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sale_ovpl_prlint"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getSale_ovpl_prlint()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sale_ovpl_intes"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getSale_ovpl_intes()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sale_overdue_prin"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getSale_overdue_prin()))));

        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("overdue_period_cnt_his"),
                Bytes.toBytes(JSON.toJSONString(StringHelper.toHashMapNotNull(record.getOverdue_period_cnt_his()))));

        table.put(put);
        System.out.println("插入/更新状态hbase:" + record.getApply_no());
    }

    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
