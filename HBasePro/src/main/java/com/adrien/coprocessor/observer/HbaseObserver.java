package com.adrien.coprocessor.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseObserver extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        List<Cell> cellList = put.get(Bytes.toBytes("info"), Bytes.toBytes("age"));
        if (cellList == null || cellList.size()== 0) {
            return;
        }

        // 如果年龄为3 我们就添加一句话
        if(3 == Bytes.toInt(CellUtil.cloneValue(cellList.get(0)))){
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("message"),Bytes.toBytes("my age is 3"));
        }
    }
}
