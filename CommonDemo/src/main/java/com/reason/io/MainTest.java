package com.reason.io;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MainTest extends AbstractRead {

    private static Set<String> dateSet = new HashSet<String>();
    private static Set<String> dateSet_2 = new HashSet<String>();
    private static long lineNumber = 0;

    // zookeeper log
    //private static String inputPath = "C:\\Users\\rd87\\Desktop\\kafka\\zookeeper\\";
    //private static String[] files = new String[]{"zk_41.log","zk_42.log","zk_43.log","zk_44.log","zk_45.log"};

    // kafka log
    private static String inputPath = "C:\\Users\\rd87\\Desktop\\kafka\\";
    private static String[] files = new String[]{"kafka_small.logej"};


    public static void main(String[] args) {
        MainTest test = new MainTest();
        for(String file : files){
            test.readFileByLines(inputPath+file);
        }

        // get zookeeper connect ip
        //test.printZookeeperConnectIP();

        // find kafka select leader error partition.
        test.printKafkaErrorPartition();

    }

    @Override
    public void executeOneLine(String oneLine) {
        System.out.println("Current line:"+ ++lineNumber);
        //getZookeeperConnectIP(oneLine);
        findKafkaPartition(oneLine);
    }


    private void printKafkaErrorPartition() {
        List<String> ls = new ArrayList<>();
        for(String data : dateSet){
            ls.add(data+" - true");
        }
        for(String data : dateSet_2){
            ls.add(data+" - false");
        }
        Collections.sort(ls);

        for(String ip : ls) {
            System.out.println(ip);
        }
        System.out.println("Totals "+ls.size());
    }


    private void findKafkaPartition(String oneLine) {

        if(oneLine.contains("Some broker in ISR is alive for")){
            String s = "";
            Pattern p = Pattern.compile("\\[\\S+,\\d+\\]");
            Matcher m = p.matcher(oneLine);
            while (m.find()) {
                s = m.group();
            }

            Pattern p_ = Pattern.compile("from ISR \\d(,\\d)*");
            Matcher m_ = p_.matcher(oneLine);
            while (m_.find()) {
                s += m_.group().substring(8);
            }
            dateSet.add(s);




        }
        if(oneLine.contains("No broker in ISR is alive for")){
            Pattern p = Pattern.compile("\\[\\S+,\\d+\\]");
            Matcher m = p.matcher(oneLine);
            while (m.find()) {
                dateSet_2.add(m.group());
            }
        }


    }


    private void printZookeeperConnectIP() {
        List<String> ls = new ArrayList<String>(dateSet);
        Collections.sort(ls);

        for(String ip : ls) {
            System.out.println(ip);
        }
        System.out.println("Total ips "+ls.size());
    }
    private void getZookeeperConnectIP(String oneLine){
        Pattern p = Pattern.compile("((?:(?:25[0-5]|2[0-4]\\d|(?:1\\d{2}|[1-9]?\\d))\\.){3}(?:25[0-5]|2[0-4]\\d|(?:1\\d{2}|[1-9]?\\d)))");
        Matcher m = p.matcher(oneLine);
        while (m.find()) {
            dateSet.add(m.group());
        }
    }




}
