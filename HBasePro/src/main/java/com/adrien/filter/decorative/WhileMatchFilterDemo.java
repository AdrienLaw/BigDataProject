package com.adrien.filter.decorative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class WhileMatchFilterDemo {
    public static void main(String[] args) {
        try { //获得Connection实例

            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            //获得Table实例
            Table table = connection.getTable(TableName.valueOf("user"));
            Scan scan = new Scan();
            Filter filter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,new BinaryComparator(Bytes.toBytes("age")));
            Filter skipfilter = new WhileMatchFilter(filter);
            scan.setFilter(skipfilter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                System.out.println(Bytes.toString(result.getRow()));
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
                    byte[] cloneValue = CellUtil.cloneValue(cell);
                    System.out.println(Bytes.toString(cloneQualifier)+ "  " + Bytes.toString(cloneValue));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
