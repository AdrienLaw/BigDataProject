package com.adrien.transform;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonInfo {
    private String name;
    private String province;
    private String city;
    private int age;
    private String idCard;

}
