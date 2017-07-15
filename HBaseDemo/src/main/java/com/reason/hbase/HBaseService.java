package com.reason.hbase;

import com.reason.util.Hash256Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by rd87 on 7/13/2017.
 */
public class HBaseService {

    private static Logger logger = Logger.getLogger(HBaseUtils.class);
    private static LinkedBlockingQueue<Map<String, String>> dataQueue = new LinkedBlockingQueue();
    private static boolean jobStatus = true;
    private static int num = 100;
    private static boolean doDel = false;
    private static int threads = 10;
    private static ExecutorService executor;
    public static void main(String[] args) {
        // E3
        //String hbaseHost = "172.16.31.81,172.16.31.82,172.16.31.83,172.16.31.84,172.16.31.85";
        // E4
        //String hbaseHost = "172.16.59.131,172.16.59.132,172.16.59.133,172.16.59.134,172.16.59.141";
        // local
        String hbaseHost = "ssspark01,ssspark03,ssspark02";
        if(args.length==3){
            System.out.println(args);
            hbaseHost = args[0];
            num =  Integer.valueOf(args[1]);
            if("del".equals(args[2])){
                doDel = true;
            }
        }
        HBaseUtils.init(hbaseHost);
        // 多线程判断是否有Review信息
        executor = Executors.newFixedThreadPool(threads+1);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                scanSummary(num);
            }
        });
        for(int i=0; i<threads; i++){
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    checkData();
                }
            });
        }
        System.out.println("--------------------------------- main is end ----------------------------");
    }

    // 如果ReviewSolr表中没有Item的Review信息，删除此Item的Summary信息
    public static void checkData(){
        logger.debug("Check thread is running.");
        while (true){
            try {
                Map<String, String> data = dataQueue.poll();
                if(!jobStatus && data == null){
                    logger.info("Check thread stop. queue size:" + dataQueue.size());
                    shutdownThreadPool();
                    break;
                }
                if(data == null){
                    Thread.sleep(500);
                    continue;
                }
                String item = data.keySet().iterator().next();
                //logger.debug("Do item:"+item);
                long checkStart = System.currentTimeMillis();
                boolean noReview = checkHasNoReview(item);
                //logger.info("Check use time "+(System.currentTimeMillis()-checkStart)+" ms.");
                if(noReview){
                    logger.error("---->"+item +" row:" + data.get(item));
                    if(doDel){
                        deleteSummary(data.get(item));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // 删除Summary信息
    private static void deleteSummary(String row) throws Exception{
        Table table = HBaseUtils.getTable("ecitem:EC_ItemReviewSummary");
        logger.info("do del row:"+row);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
        HBaseUtils.closeTable(table);
    }

    //scan summary 表中的所有信息
    private static void scanSummary(int n){
        try {
            logger.error("----------------------------------------------- start -----------------------------------------------");
            long start = System.currentTimeMillis();
            Table table = HBaseUtils.getTable("ecitem:EC_ItemReviewSummary");

            Scan scan = new Scan();
            scan.setCaching(1000);
            scan.addColumn(Bytes.toBytes("BaseInfo"),Bytes.toBytes("Item"));
            ResultScanner results = table.getScanner(scan);
            int i = 0;
            while (n > i++){
                String item;
                Result result = results.next();
                if(result == null){
                    break;
                }
                item = Bytes.toString(result.getValue(Bytes.toBytes("BaseInfo"),Bytes.toBytes("Item")));
                logger.info("-----"+i+"-----> item:"+item);
                try {
                    String parent = getParent(Bytes.toString(result.getRow()).substring(0,64));
                    if(StringUtils.isNotBlank(parent)){
                        item = parent;
                    }
                } catch (Exception e) {
                    logger.warn("Get parent error", e);
                }
                Map<String, String> data = new HashMap<>();
                data.put(item, Bytes.toString(result.getRow()));
                dataQueue.put(data);
            }
            HBaseUtils.closeTable(table);
            jobStatus = false;
            logger.info("--------------------------- Scan job end use time "+(System.currentTimeMillis() - start) + " ms. -----------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean checkHasNoReview(String item) throws Exception{
        boolean noReview = false;
        Table table = HBaseUtils.getTable("ecitem:EC_ItemReviewSolr");
        Scan scan = new Scan();
        scan.setCaching(5);
        String row = Hash256Util.gethashCode(item);
        scan.setRowPrefixFilter(Bytes.toBytes(row));
        scan.addColumn(Bytes.toBytes("BaseInfo"),Bytes.toBytes("item"));
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("BaseInfo"), Bytes.toBytes("Approve"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("Y")));
        ResultScanner results = table.getScanner(scan);
        Result result = results.next();
        noReview = result == null;
        HBaseUtils.closeTable(table);
        return noReview;
    }

    public static String getParent(String row) throws Exception{
        Table table = HBaseUtils.getTable("ecitem:IM_ItemBase");
        Get get = new Get(Bytes.toBytes(row));
        get.addColumn(Bytes.toBytes("BaseInfo"),Bytes.toBytes("ParentItem"));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes("BaseInfo"),Bytes.toBytes("ParentItem"));
        HBaseUtils.closeTable(table);
      return Bytes.toString(value);
    }


    public static void shutdownThreadPool(){
        if(!executor.isShutdown()){
            logger.info("Thread pool shutdown");
            executor.shutdown();
        }
    }

}
