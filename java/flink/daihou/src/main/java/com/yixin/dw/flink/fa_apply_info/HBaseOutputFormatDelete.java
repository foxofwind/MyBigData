package com.yixin.dw.flink.fa_apply_info;

import com.yixin.dw.flink.domain.FaApplyInfoExtOut;
import com.yixin.dw.flink.util.StringHelper;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Create By 鸣宇淳 on 2020/2/11
 **/
public class HBaseOutputFormatDelete implements OutputFormat<FaApplyInfoExtOut> {
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
        Delete delete=new Delete(Bytes.toBytes(record.getID()));
        table.delete(delete);
        System.out.println("删除hbase:" + record.getID());
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
