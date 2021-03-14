package com.adrien.filter.value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiRowRangeFilterDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            // 构造RowRange 四个参数分别为startRow startRowInclusive(是否包含起始行) stopRow stopRowInclusive(是否包含结束行)
            MultiRowRangeFilter.RowRange rowRange1 = new MultiRowRangeFilter.RowRange("20001",true,"20002",false);
            MultiRowRangeFilter.RowRange rowRange2 = new MultiRowRangeFilter.RowRange("20002",false,"20007",true);
            List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
            rowRanges.add(rowRange1);
            rowRanges.add(rowRange2);
            Filter filter = new MultiRowRangeFilter(rowRanges);
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
