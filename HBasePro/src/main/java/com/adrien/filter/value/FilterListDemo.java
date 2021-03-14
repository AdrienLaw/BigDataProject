package com.adrien.filter.value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class FilterListDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            ArrayList<Filter> filters = new ArrayList<>();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL,new SubstringComparator("adrien"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(22)));
            filters.add(filter1);
            filters.add(filter2);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,filters);
            scan.setFilter(filterList);
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
