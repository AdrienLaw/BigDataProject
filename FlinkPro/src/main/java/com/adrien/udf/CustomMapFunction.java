package com.adrien.udf;

import com.adrien.sources.SensorReading;
import com.adrien.sources.SensorReading2;
import org.apache.flink.api.common.functions.MapFunction;

public class CustomMapFunction implements MapFunction<SensorReading,String> {

    @Override
    public String map(SensorReading sensorReading) throws Exception {
        return sensorReading.getName() + "==" + sensorReading.getADouble();
    }
}
