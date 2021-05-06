package com.adrien.sources;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor                 //无参构造
@AllArgsConstructor                //有参构造
public class SensorReading {
    private String name;
    private Long along;
    private Double aDouble;
}
