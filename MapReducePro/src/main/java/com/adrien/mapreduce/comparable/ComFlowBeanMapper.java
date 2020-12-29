package com.adrien.mapreduce.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 13470253144	180	180	360
 * 13509468723	7335	110349	117684
 * 13560439638	918	4938	5856
 * 13568436656	3597	25635	29232
 * 13590439668	1116	954	2070
 * 13630577991	6960	690	7650
 * 13682846555	1938	2910	4848
 *
 * 排序
 * 13509468723	ComFlowBean{upFlow=7335, downFlow=110349, sumFlow=117684}
 * 13975057813	ComFlowBean{upFlow=11058, downFlow=48243, sumFlow=59301}
 * 13568436656	ComFlowBean{upFlow=3597, downFlow=25635, sumFlow=29232}
 * 13736230513	ComFlowBean{upFlow=2481, downFlow=24681, sumFlow=27162}
 * 18390173782	ComFlowBean{upFlow=9531, downFlow=2412, sumFlow=11943}
 * 13630577991	ComFlowBean{upFlow=6960, downFlow=690, sumFlow=7650}
 * 15043685818	ComFlowBean{upFlow=3659, downFlow=3538, sumFlow=7197}
 */
public class ComFlowBeanMapper extends Mapper<LongWritable,Text,ComFlowBean,Text> {
    ComFlowBean comFlowBean = new ComFlowBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String string = value.toString();
        //切割
        String[] splits = string.split("\t");
        String phoneNum = splits[0];
        long upFlow = Long.valueOf(splits[1]);
        long downFlow = Long.valueOf(splits[2]);
        long sumFlow = Long.valueOf(splits[3]);
        comFlowBean.setUpFlow(upFlow);
        comFlowBean.setDownFlow(downFlow);
        comFlowBean.setSumFlow(sumFlow);
        text.set(phoneNum);
        context.write(comFlowBean,text);
    }
}
