package com.adrien.filter.value;

import com.adrien.basic.HBaseDML;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ValueFilterDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("adrien"));
            scan.setFilter(filter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
                System.out.println(name);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
