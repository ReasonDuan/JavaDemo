package com.reason.io;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MainTest extends AbstractRead {

    private static Set<String> allIP = new HashSet<String>();
    private static long lineNumber = 0;


    public static void main(String[] args) {
        MainTest test = new MainTest();
        test.readFileByLines("C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\zk_41.log");
        test.readFileByLines("C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\zk_42.log");
        test.readFileByLines("C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\zk_43.log");
        test.readFileByLines("C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\zk_44.log");
        test.readFileByLines("C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\zk_45.log");

        List<String> ls = new ArrayList<String>(allIP);
        Collections.sort(ls);

        for(String ip : ls) {
            System.out.println(ip);
        }
        System.out.println("Total ips "+ls.size());

    }


    public static void test(){
        String str = "yin yu shi wo zui cai de yu yan";
        System.out.println(str);
        String reg = "\\b[a-z]{3}\\b";//匹配只有三个字母的单词

        //将规则封装成对象。
        Pattern p = Pattern.compile(reg);

        //让正则对象和要作用的字符串相关联。获取匹配器对象。
        Matcher m  = p.matcher(str);

        //System.out.println(m.matches());//其实String类中的matches方法。用的就是Pattern和Matcher对象来完成的。
        //只不过被String的方法封装后，用起来较为简单。但是功能却单一。

        // boolean b = m.find();//将规则作用到字符串上，并进行符合规则的子串查找。
        // System.out.println(b);
        // System.out.println(m.group());//用于获取匹配后结果。


        while(m.find())
        {
            System.out.println(m.group());
            System.out.println(m.start()+"...."+m.end());
            // start()  字符的开始下标（包含）
            //end()  字符的结束下标（不包含）
        }
    }




    @Override
    public void executeOneLine(String oneLine) {
        Pattern p = Pattern.compile("((?:(?:25[0-5]|2[0-4]\\d|(?:1\\d{2}|[1-9]?\\d))\\.){3}(?:25[0-5]|2[0-4]\\d|(?:1\\d{2}|[1-9]?\\d)))");
        Matcher m = p.matcher(oneLine);
        while (m.find()) {
            allIP.add(m.group());
        }
        System.out.println("Current line:"+ ++lineNumber);
    }
}
