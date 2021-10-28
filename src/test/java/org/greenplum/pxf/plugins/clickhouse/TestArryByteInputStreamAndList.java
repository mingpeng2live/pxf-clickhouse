package org.greenplum.pxf.plugins.clickhouse;

import ru.yandex.clickhouse.response.FastByteArrayOutputStream;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author pengming  
 * @date 2021年10月27日 3:12 下午
 * @description
 */
public class TestArryByteInputStreamAndList {


    public static void main(String[] args) throws Exception {

        String str;
        int length = 100000;
        FastByteArrayOutputStream fbao = new FastByteArrayOutputStream();

        ByteArrayOutputStream fout = new ByteArrayOutputStream();
//        str = "['{\"206_sender\":\"pablo\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']";
        List<byte[]> list = new ArrayList<>(length);
        long s = System.currentTimeMillis();

//        for (int i = 0; i < length; i++) {
//            str = new String(i + "['{\"206_sender\":\"pablo\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
//            fbao.write(str.getBytes(StandardCharsets.UTF_8));
//        }
//        fbao.writeTo(fout);

        for (int i = 0; i < length; i++) {
            str = new String(i + "['{\"206_sender\":\"pablo\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
            list.add(str.getBytes(StandardCharsets.UTF_8));
        }
        for (byte[] bytes : list) {
            fout.write(bytes);
        }
        long e = System.currentTimeMillis();
        System.out.println("fast: " + (e - s));

    }


}
