package com.adrien.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import javax.annotation.Tainted;
import java.io.IOException;

public class HBaseDML {


    private static Connection init () throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    private static void destroy (Table table) {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void insertData () {
        try {
            Connection connection = init();
            Table table = connection.getTable(TableName.valueOf("test"));
            Put put = new Put(Bytes.toBytes("40001"));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(3));
            table.put(put);
            destroy(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData() {
        try {
            Connection connection = init();
            Table table = connection.getTable(TableName.valueOf("test"));
            Get get = new Get(Bytes.toBytes("20001"));
            get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                long timestamp = cell.getTimestamp();
                System.out.println(family + " " + column + " " + value + " " + timestamp);
            }
            destroy(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void scanTable () {
        try {
            Connection connection = init();
            Table table = connection.getTable(TableName.valueOf("test"));
            //输出 20001 - 20002 - 20003
            Scan scan = new Scan(Bytes.toBytes("40001"), Bytes.toBytes("40004"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            //设置是否缓存，默认是true。但是mr不是热数据，可以用false。
            scan.setCacheBlocks(false);
            //每次从服务器端读取的行数，大了占内存或者超时,OOM，但小了机能不好。
            scan.setCaching(10);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    System.out.println(family + " " + column + " " + value + " " + timestamp);
                }
            }
            destroy(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 删除表中数据
     */
    @Test
    public void deleteTable () {
        try {
            Connection connection = init();
            Table table = connection.getTable(TableName.valueOf("test"));
            Delete delete = new Delete(Bytes.toBytes("20003"));
            table.delete(delete);
            destroy(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
