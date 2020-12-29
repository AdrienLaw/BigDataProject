package com.adrien.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**id   手机号码      网络 ip                         上行流量  下行流量  网络状态码
 * 5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
 * 6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
 */
public class FlowCountMapper extends Mapper<LongWritable,Text, Text,FlowBean> {
    //输出对的 对象
    Text text = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取  切割 数据
        String line = value.toString();
        String[] splits = line.split("\t");
        //输出的 Key 手机号
        text.set(splits[1]);
        long upflow = Long.parseLong(splits[splits.length - 3]);
        long downflow = Long.parseLong(splits[splits.length - 2]);
        flowBean.setUpFlow(upflow);
        flowBean.setDownFlow(downflow);
        context.write(text,flowBean);
    }
}
