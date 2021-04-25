package com.adrien.sql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ToLowerCaseUDF extends UDF {
    public Text evaluate(Text text) {
        if (text == null) {
            return null;
        }
        if (text != null && text.toString().length() <= 0) {
            return null;
        }
        return new Text(text.toString().toLowerCase());
    }

    public static void main(String[] args) {
        Text str = new ToLowerCaseUDF().evaluate(new Text("FUCK YOU"));
        System.out.println(str.toString());
    }

}
