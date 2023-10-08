package org.apache.nifi.processors.doris;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONUtil;
import org.junit.Test;

import java.net.http.HttpClient;
import java.util.HashMap;

public class ParseJSONTest {

    @Test
    public void test(){

        String s = "{\"type\":\"update\",\"timestamp\":1685522828000,\"binlog_filename\":\"master.000003\",\"binlog_position\":512646914,\"database\":\"flinkcdc1\",\"table_name\":\"test1\",\"table_id\":210,\"columns\":[{\"id\":1,\"name\":\"a\",\"column_type\":12,\"last_value\":\"ssdsad\",\"value\":\"ssdsad\"},{\"id\":2,\"name\":\"b\",\"column_type\":4,\"last_value\":200,\"value\":200},{\"id\":3,\"name\":\"c\",\"column_type\":12,\"last_value\":\"d\",\"value\":\"d\"},{\"id\":4,\"name\":\"d\",\"column_type\":12,\"last_value\":\"d\",\"value\":\"dd\"}]}";

    }

    @Test
    public void test2(){
        HashMap<String, HttpClient> stringStringHashMap = new HashMap<>();

        HttpClient sss = stringStringHashMap.get("sss");

        System.out.println("sss = " + sss);


    }

}
