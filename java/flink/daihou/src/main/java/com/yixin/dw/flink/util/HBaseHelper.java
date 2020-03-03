package com.yixin.dw.flink.util;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Create By 鸣宇淳 on 2020/2/19
 **/
public class HBaseHelper {
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private static Map<String, Table> tableMap = new HashMap<>();

    public HBaseHelper(String hbaseTableName) throws IOException {
        conf = HBaseConfiguration.create();

        conf.set("zookeeper.znode.parent", "/hbase2-secure");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.0.4.91,10.0.4.90,10.0.4.92");
        conf.set("hbase.client.retries.number", "1");

        conn = ConnectionFactory.createConnection(conf);
        if (!tableMap.containsKey(hbaseTableName)) {
            tableMap.put(hbaseTableName, conn.getTable(TableName.valueOf(hbaseTableName)));
        }
    }

    public Result getByRowKey(String hbaseTableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return tableMap.get(hbaseTableName).get(get);
    }
}
