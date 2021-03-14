package com.adrien.filter.value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SingleColumnValueFilterDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL, new SubstringComparator("rien"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("adrien")));
//            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
//                    CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("d")));
            scan.setFilter(filter2);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
                System.out.println(name);
            }

            BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("adrien"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
