package com.reason;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSONObject;
import com.reason.util.RandomUserUtil;
import com.reason.util.RandomUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class ProducerDemo {

    private static Logger logger = Logger.getLogger(ProducerDemo.class);

    public final static String ZKHOSTS = "10.16.238.92:29092";

    public final static String TOPIC = "reason-user";
    // 开启线程数
    private final static int THREADS = 1;
    // 每个线程生产消息数
    private final static int MSGS = 100;
    // 每次生产玩，休眠时间 ms
    public final static int SLEEPTIME = 100;

    /**
     * 发送消息
     */
    public void producer() {
        Producer<String, String> producer = getProducer();
        long count = 0;
        int i=MSGS;
        logger.debug("Producer ***************** start.");
        while(i>0){
            sendMessage(producer);
            try {Thread.sleep(SLEEPTIME);} catch (Exception e) {}
            count++;
            if(count%(MSGS/10)==0){
                logger.debug("Producer message size:"+count);
            }
            i--;
        }
        logger.debug("Producer ***************** end.");
        producer.close();
    }
    /**
     * 创建一个producer
     * @return
     */
    public static Producer<String, String> getProducer(){
        Properties props = new Properties();

        props.put("bootstrap.servers", ZKHOSTS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);
        return producer;
    }
    /**
     * 用producer发送消息
     * @param producer
     */
    public void sendMessage(Producer<String, String> producer){
        try {
            producer.send(new ProducerRecord(TOPIC, RandomUtil.getRandomString(5), JSONObject.toJSONString(RandomUserUtil.get())));
        } catch (Exception e) {
            logger.error("System error!<----------->");
            e.printStackTrace();
            try {Thread.sleep(100);} catch (Exception ee) {}
            producer = getProducer();
            sendMessage(producer);
        }
    }

    public static void main(String[] args) {
        //logger.debug("Producer ***************** start.");
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        long start = System.currentTimeMillis();
        for(int i=0; i<THREADS; i++){
            executorService.execute(new Runnable() {
                public void run() {
                    new ProducerDemo().producer();
                }
            });
        }
        executorService.shutdown();

        while (!executorService.isTerminated()) {}
        logger.info("Total time:"+(System.currentTimeMillis() - start));
        //logger.debug("Producer ***************** end.");
    }

}
