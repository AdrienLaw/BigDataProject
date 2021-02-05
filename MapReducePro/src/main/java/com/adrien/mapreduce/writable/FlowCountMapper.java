package com.adrien.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**id   手机号码      网络 ip                         上行流量  下行流量  网络状态码
 * 5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
 * 6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
 *
 * 13470253144	FlowBean{upFlow=180, downFlow=180, sumFlow=360}
 * 13509468723	FlowBean{upFlow=7335, downFlow=110349, sumFlow=117684}
 * 13560439638	FlowBean{upFlow=918, downFlow=4938, sumFlow=5856}
 * 13568436656	FlowBean{upFlow=3597, downFlow=25635, sumFlow=29232}
 * 13590439668	FlowBean{upFlow=1116, downFlow=954, sumFlow=2070}
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
