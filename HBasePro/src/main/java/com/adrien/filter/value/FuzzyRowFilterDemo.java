package com.adrien.filter.value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FuzzyRowFilterDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181, hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection (configuration);
            Table table = connection.getTable(TableName.valueOf ("test"));
            Scan scan = new Scan();
            List<Pair<byte[], byte[]>> fuzzyKeysData = new ArrayList<>();
            Pair<byte[], byte[]> pair = new Pair<>(
                    Bytes.toBytes("2021_??_??_0001"),
                    new byte[]{0,0,0,0,0,1,1,0,1,1,0,0,0,0,0}
            );
            fuzzyKeysData.add(pair);
            FuzzyRowFilter fuzzyRowFilter = new FuzzyRowFilter(fuzzyKeysData);
            scan.setFilter(fuzzyRowFilter);
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
