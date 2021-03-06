package com.adrien.filter.column;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 列名过滤器
 */
public class KeyOnlyFilterDemo {
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("user"));
            Scan scan = new Scan();
            KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
            scan.setFilter(keyOnlyFilter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                Cell[] cells = result.rawCells();
                for(Cell cell:cells){
                    byte[] bytes =CellUtil.cloneQualifier(cell);
                    System.out.println(Bytes.toString(bytes));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
