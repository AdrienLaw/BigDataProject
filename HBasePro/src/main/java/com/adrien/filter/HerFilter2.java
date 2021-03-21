package com.adrien.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HerFilter2 extends FilterBase {

    // 需要比较的列族名
    private String family;
    // 数学成绩列
    private String columnA;
    // 语文成绩列
    private String columnB;
    // 对应列的值
    private Integer columnAValue;
    private Integer columnBValue;

    public HerFilter2(String columnA, String columnB) {
        this.columnA = columnA;
        this.columnB = columnB;
    }

    /**
     * filterKeyValue会遍历所有的keyvalue并把符合条件的放入结果集
     * 因此我们需要做的就是：当它遍历到columnA columnB 的时候 把value记录下来 进行比较
     *
     * @param cell
     * @return
     * @throws IOException
     */
    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        if (CellUtil.matchingColumn(cell, Bytes.toBytes(family), Bytes.toBytes(columnA))) {
            columnAValue = Bytes.toInt(CellUtil.cloneValue(cell));
        }
        if (CellUtil.matchingColumn(cell, Bytes.toBytes(family), Bytes.toBytes(columnB))) {
            columnBValue = Bytes.toInt(CellUtil.cloneValue(cell));
        }
        // 表示结果中要包含这个KeyValue
        return ReturnCode.INCLUDE;
    }

    /**
     * 比较数学成绩是否大于语文成绩
     * filterRow会在所有keyvalue被遍历完之后决定是否需要过滤
     *
     * @return
     * @throws IOException
     */
    @Override
    public boolean filterRow() throws IOException {
        return columnAValue > columnBValue;
    }

    /**
     * 如果是针对行级别的过滤方法那么必须实现这个方法
     * @return
     */
    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return super.toByteArray();
    }


}
