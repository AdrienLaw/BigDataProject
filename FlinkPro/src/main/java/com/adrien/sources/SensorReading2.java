package com.adrien.sources;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor                 //无参构造
@AllArgsConstructor                //有参构造
public class SensorReading2 {
    private String id;
    private Long timestamp;
    private Double temperature;
}
