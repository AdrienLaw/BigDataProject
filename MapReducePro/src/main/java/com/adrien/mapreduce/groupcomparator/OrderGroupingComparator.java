package com.adrien.mapreduce.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {
    protected OrderGroupingComparator () {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int res;
        OrderBean orderBeanA = (OrderBean) a;
        OrderBean orderBeanB = (OrderBean) b;
        if (orderBeanA.getOrderId() > orderBeanB.getOrderId()) {
            res = 1;
        } else if (orderBeanA.getOrderId() <  orderBeanB.getOrderId()) {
            res =  -1;
        } else {
            res = 0;
        }
        return res;
    }
}
