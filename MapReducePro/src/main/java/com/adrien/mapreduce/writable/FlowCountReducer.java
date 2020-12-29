package com.adrien.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean, Text,FlowBean> {

    Text text = new Text();
    FlowBean flowBean =  new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_up = 0;
        long sum_down = 0;
        for (FlowBean value : values) {
            sum_up += value.getUpFlow();
            sum_down += value.getDownFlow();

        }

        flowBean.set(sum_up,sum_down);
        context.write(key,flowBean);
    }
}
