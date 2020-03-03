package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.domain.FaApplyInfoExtOut;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Create By 鸣宇淳 on 2020/2/11
 **/
public class HBaseOutputFormatInUpdate implements OutputFormat<FaApplyInfoExtOut> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseOutputFormatInUpdate.class);

    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;
    private final String hbaseTableName = "RT:testSinkTable";

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
    public void writeRecord(FaApplyInfoExtOut record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.getID()));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("apply_id"), Bytes.toBytes(StringHelper.toStringNotNull(record.getApply_id())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("repaied_nper"), Bytes.toBytes(StringHelper.toStringNotNull(record.getRepaied_nper())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("remain_period"), Bytes.toBytes(StringHelper.toStringNotNull(record.getRemain_period())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("residual_principal"), Bytes.toBytes(StringHelper.toStringNotNull(record.getResidual_principal())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("overdue_level"), Bytes.toBytes(StringHelper.toStringNotNull(record.getOverdue_level())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("overdue_days"), Bytes.toBytes(StringHelper.toStringNotNull(record.getOverdue_days())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("overdue_nper"), Bytes.toBytes(StringHelper.toStringNotNull(record.getOverdue_nper())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("repay_date"), Bytes.toBytes(StringHelper.toStringNotNull(record.getRepay_date())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("repay_status"), Bytes.toBytes(StringHelper.toStringNotNull(record.getRepay_status())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("repay_status_name"), Bytes.toBytes(StringHelper.toStringNotNull(record.getRepay_status_name())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("is_overdue"), Bytes.toBytes(StringHelper.toStringNotNull(record.getIs_overdue())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("clear_type_id"), Bytes.toBytes(StringHelper.toStringNotNull(record.getClear_type_id())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("clear_type_name"), Bytes.toBytes(StringHelper.toStringNotNull(record.getClear_type_name())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("clear_time"), Bytes.toBytes(StringHelper.toStringNotNull(record.getClear_time())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("is_repurchased"), Bytes.toBytes(StringHelper.toStringNotNull(record.getIs_repurchased())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sale_overdue_amt"), Bytes.toBytes(StringHelper.toStringNotNull(record.getSale_overdue_amt())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("alrepaid_amt"), Bytes.toBytes(StringHelper.toStringNotNull(record.getAlrepaid_amt())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("overdue_month_cnt"), Bytes.toBytes(StringHelper.toStringNotNull(record.getOverdue_month_cnt())));

        table.put(put);
        System.out.println("插入/更新hbase:" + record.getID());
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
