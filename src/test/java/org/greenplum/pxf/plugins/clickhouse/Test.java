package org.greenplum.pxf.plugins.clickhouse;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.plugins.clickhouse.utils.JackSonUtils;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author pengming  
 * @date 2021年06月02日 3:45 下午
 * @description
 */




public class Test {

    public static void main(String[] args) throws Exception {



//        System.out.println(BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234,127.0.0.1:4321/ppc?user=22&pass=44"));

//        String d = "{\"s\",\"d\",\"c\"}";
//        String d = "[2,3,4]";
//        String d = "{Y,H,H,Y}";
//        String d = "{Y,H,H,Y}";
//        String d = "{1,3,4}";
        String d = "{}";

        d = d.substring(1, d.length() - 1);
        System.out.println("d: " + d);
//        Object[] json = StringUtils.split(d, ",");
//        System.out.println(JackSonUtils.toJsonString(json));




//        d = "[" + d + "]";
//        json = JackSonUtils.jsonToClass(d, new TypeReference<Integer[]>() {
//        });
//        System.out.println(JackSonUtils.toJsonString(json));


//        d = "[" + d + "]";
//        json = JackSonUtils.jsonToClass(d, new TypeReference<Integer[]>() {
//        });
//        System.out.println(JackSonUtils.toJsonString(json));
//
//      {"{\"sender\":\"pablo\",\"body\":\"they are on to us\"}","{\"sender\":\"arthur\"}"}
//        String c = "{\"{\\\"sender\\\":\\\"pablo\\\",\\\"body\\\":\\\"they are on to us\\\"}\",\"{\\\"sender\\\":\\\"arthur\\\"}\"}";
//        String c = "{\"\",NULL,\"\",\"d,d\",NULL,\"c,\\\"r\",NULL,\"\"}";
//        String c = "{\"{\\\"sender\\\":\\\"pablo\\\",\\\"body\\\":\\\"they are on to us\\\"}\"}";
//        String c = "{\"{\\\"sender\\\":\\\"pablo\\\",\\\"body\\\":\\\"they are \\\\\\\"on to us\\\"}\",NULL,\"{\\\"sender\\\":\\\"arthur\\\"}\"}";
//        String c = "{aaa,\"adc,afe\",\"{\\\"sender\\\":\\\"pablo\\\",\\\"body\\\":\\\"they are on to us\\\"}\",NULL,\"{\\\"sender\\\":\\\"arthur\\\"}\"}";
        String c = "{aaa,ddd,null,\"adc,afe\"}";
        c = c.substring(1, c.length() - 1);
//        c = "[" + c + "]";
//
//        json = JackSonUtils.jsonToClass(c, new TypeReference<String[]>() {
//        });
//        System.out.println(JackSonUtils.toJsonString(json));
//
//
//        c = "{\"2021-06-17 18:02:17.435\",\"2021-06-15 18:02:17.435\"}";
//        c = "{t,f}";
//        c = "['aa,f',NULL,'cc,c','dd']";
//        c = c.substring(2, c.length() - 2);
//        c = c.substring(1, c.length() - 1);
//        c = "[" + c + "]";


        String[] splitStrArray = ClickHouseResolver.splitStrArray(c);
        System.out.println("size: " + splitStrArray.length + " val: " + JackSonUtils.toJsonString(splitStrArray));

//        String[] split = ClickHouseResolver.split(c, "\",\"", false, false);
//        String[] split = ClickHouseResolver.splitReg(c, true, ClickHouseResolver.psJson);
//        System.out.println(JackSonUtils.toJsonString(split));


//        TypeReference<Timestamp[]> typeReference = new TypeReference<Timestamp[]>() {
//        };
//
//        System.out.println(typeReference.getType().getTypeName());
//
//        Timestamp[] json = JackSonUtils.jsonToClass(c, typeReference);
//        System.out.println(JackSonUtils.toJsonString(json));
//
//
//        Date[] dates = new Date[]{new Date(),new Date()};
//        System.out.println(JackSonUtils.toJsonString(dates));
//
//
//
//        System.out.println(ArrayUtils.toString(json));
//
//
//        System.out.println("dddd:\t" + "[]".substring(1, "[]".length() - 1));
//        List s = new ArrayList();
//        s.add("s");
//        s.add("d");
//        s.add("c");
//        System.out.println(s);

    }


}
