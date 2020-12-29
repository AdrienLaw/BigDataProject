package com.adrien.mapreduce.partitioner;

import com.adrien.mapreduce.comparable.ComFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.codehaus.jackson.map.ser.std.ScalarSerializerBase;

public class SubThreePartitioner extends Partitioner<ComFlowBean, Text> {

    @Override
    public int getPartition(ComFlowBean comFlowBean, Text text, int numPartitions) {
        String subThree = text.toString().substring(0, 3);
        int partition = 4;
        if ("136".equals(subThree)) {
            partition = 0;
        } else if ("137".equals(subThree)) {
            partition = 1;
        } else if ("138".equals(subThree)) {
            partition = 2;
        } else if ("139".equals(subThree)) {
            partition = 3;
        }
        return partition;
    }
}
