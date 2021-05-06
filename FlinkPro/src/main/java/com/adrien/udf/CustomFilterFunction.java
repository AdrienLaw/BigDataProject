package com.adrien.udf;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.logging.Filter;

public class CustomFilterFunction implements FilterFunction<SensorReading> {
    @Override
    public boolean filter(SensorReading sensorReading) throws Exception {
        return sensorReading.getADouble() > 20;
    }
}
