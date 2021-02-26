package com.adrien.jdbc;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static com.adrien.jdbc.PlatformDictionary.DRIVERNAME;

public class HiveJDBC {



    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet resultSet = null;


    /**
     * 加载驱动 创建连接
     */
    private static void init () {
        try {
            Class.forName(DRIVERNAME);
            conn = DriverManager.getConnection(PlatformDictionary.CONNECTION  ,PlatformDictionary.USERNAME, PlatformDictionary.PASSWORD);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            Object abnormal = AbnormalUtils.getAbnormal(e);
            System.err.println(abnormal);
        }
    }

    /**
     * 释放资源
     */
    public static void destroy () {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException throwables) {
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        }
    }


    /**
     * 创建数据库
     */
    @Test
    public void createDatabase () {
        try {
            init();
            String sql = "create database easyEs";
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }
    }

    @Test
    public void showDatabases () {
        try {
            List<Object> list = new ArrayList<>();
            init();
            String sql = "show databases";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
            for (Object o : list) {
                System.out.println(o.toString());
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            destroy();
        }

    }


    @Test
    public void dropDatabase () {
        try {
            init();
            String sql = "drop database if exists easyEs";
            stmt.execute(sql);
        } catch (SQLException throwables) {
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }
    }




    /**
     * 创建表
     */
    @Test
    public void createTable () {
        try {
            init();
            String useSql = "use db_web_data";
            stmt.execute(useSql);
            String createSql = "CREATE EXTERNAL TABLE IF NOT EXISTS `dmp_clearlog` (\n" +
                    "  `date_log` string COMMENT 'date in file', \n" +
                    "  `hour` int COMMENT 'hour', \n" +
                    "  `device_id` string COMMENT '(android) md5 imei / (ios) origin  mac', \n" +
                    "  `imei_orgin` string COMMENT 'origin value of imei', \n" +
                    "  `mac_orgin` string COMMENT 'origin value of mac', \n" +
                    "  `mac_md5` string COMMENT 'mac after md5 encrypt', \n" +
                    "  `android_id` string COMMENT 'androidid', \n" +
                    "  `os` string  COMMENT 'operating system', \n" +
                    "  `ip` string COMMENT 'remote real ip', \n" +
                    "  `app` string COMMENT 'appname' )\n" +
                    "COMMENT 'cleared log of origin log'\n" +
                    "PARTITIONED BY (\n" +
                    "  `date` date COMMENT 'date used by partition'\n" +
                    ")\n" +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \n" +
                    "TBLPROPERTIES ('creator'='szh', 'crate_time'='2018-06-07')\n";
            stmt.execute(createSql);
        } catch (SQLException throwables) {
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }
    }

    @Test
    public void showTables () {
        ArrayList<Object> list = new ArrayList<>();
        try {
            init();
            String useSql = "use db_web_data";
            stmt.execute(useSql);
            String showSqp = "show tables";
            resultSet = stmt.executeQuery(showSqp);
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
            for (Object o : list) {
                System.out.println(o.toString());
            }
        } catch (SQLException throwables) {
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }

    }


    /**
     * 查看表结构
     */
    @Test
    public void descTable () {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> map = null;
        try {
            init();
            String sql = "desc db_web_data.track_log";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                map = new HashMap<>();
                map.put("colName", resultSet.getString(1));
                map.put("dataType", resultSet.getString(2));
                list.add(map);
            }
            for (Map<String, String> stringObjectMap : list) {
                for (Map.Entry<String, String> stringObjectEntry : map.entrySet()) {
                    String key = stringObjectEntry.getKey();
                    String value = stringObjectEntry.getValue();
                    System.out.println(key + " : " + value);
                    System.out.println("==");
                }
            }
        } catch (SQLException throwables) {
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }
    }


    /**
     * 加载数据
     */
    @Test
    public void loadData () {
        try {
            init();
            String hdfsPath = "/hive_operate/rating_table";
            String sql = "load data inpath '" + hdfsPath + "' OVERWRITE into table douban.rating_table";
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Object abnormal = AbnormalUtils.getAbnormal(throwables);
            System.err.println(abnormal);
        } finally {
            destroy();
        }
    }


    @Test
    public void selectData () {
        ArrayList<Object> list = new ArrayList<>();
        try {
            init();
            String str = "select * from db_hive_demo.emp";
            resultSet = stmt.executeQuery(str);
            while (resultSet.next()) {
                list.add(resultSet.getString(1) + " | " + resultSet.getString(2) + " | " + resultSet.getString(3)
                        + " | " + resultSet.getString(4));
            }
            for (Object o : list) {
                System.out.println(o.toString());
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            destroy();
        }
    }







    @Test
    public void getTableCount () {
        try {
            Class.forName(DRIVERNAME);
            String database = "db_hive_demo";
            Connection con = DriverManager.getConnection(PlatformDictionary.CONNECTION  ,PlatformDictionary.USERNAME, PlatformDictionary.PASSWORD);
            Statement stmt = con.createStatement();
            String tableName = "db_hive_demo.emp";
            String sql = "select count(*) from " + tableName;
            ResultSet rs = stmt.executeQuery(sql);
            if(rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


}
