package org.greenplum.pxf.plugins.clickhouse;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author pengming  
 * @date 2021年08月18日 7:15 下午
 * @description
 */
public class Ts {

    class A {
        private void c() {
            System.out.println("a c");
        }
    }


    class B extends A {
        public void c() {
            System.out.println("b c");
        }
    }

    public void d() {
        A a = new B();
        a.c();
    }


    static List<Character> c = new ArrayList<>();
    static {
        c.add(' ');
        c.add('\n');
        c.add('\t');
        c.add('\r');
        c.add('\f');
    }

    public static String trim(String value) {
        int len = value.length();
        int st = 0;
        while ((st < len) && c.contains(value.charAt(st))) {
            st++;
        }
        while ((st < len) && c.contains(value.charAt(len - 1))) {
            len--;
        }
        return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
    }


    public static void main(String[] args) {
        String a = " \033dd a\033";

        System.out.println(trim(a));
//        System.out.println(a.replaceAll("^\\s*|\\s*$", ""));


    }

}
