package com.adrien.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

public class HBaseDDL {

    private static Admin init () throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        return admin;
    }

    private static void close () {

    }

    @Test
    public void createTable () {
        try {
            Admin admin = init();
            TableName tableName = TableName.valueOf("java_admin");
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor("info"));
                admin.createTable(tableDescriptor);
                System.out.println("=== success!! ====");
            } else {
                System.out.println("Table is already existed!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void dropTable (){
        try {
            Admin admin = init();
            if (!admin.isTableDisabled(TableName.valueOf("java_admin"))) {
                admin.disableTable(TableName.valueOf("java_admin"));
            }
            admin.deleteTable(TableName.valueOf("java_admin"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
