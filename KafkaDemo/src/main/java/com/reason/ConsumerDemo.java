package com.reason;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.log4j.Logger;

/**
 *
 * kafka_version : 0.10.2.0
 *
 *
 */
public class ConsumerDemo {
    private static Logger logger = Logger.getLogger(ConsumerDemo.class);

    // public final static String TOPIC = "EC_REPLICATION";
    public final static String TOPIC = "reason-user";
    public final static int PARTITIONS = 3;
    public final static int GETMSGS = 1000;
    public final static int SLEEPTIME = 300;
    public final static String ZKHOST = "10.16.238.101:8181";
    public final static String GROUP = "reason-group";

    public ConsumerConnector getConsumer() {
        Properties props = new Properties();
        // zookeeper 配置

        // 设置zookeeper的链接地址
        props.put("zookeeper.connect", ZKHOST);
        // group 代表一个消费组 // 设置group id
        props.put("group.id", GROUP);
        // 等待重新平衡
        props.put("rebalance.backoff.ms", "8000");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        // kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
        props.put("auto.commit.interval.ms", "1000");
        // props.put("auto.offset.reset", "smallest");
        // props.put("auto.offset.reset", "largest");
        // 序列化类
        // props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);
        return Consumer.createJavaConsumerConnector(config);
    }

    public class ConsumerThread implements Runnable{
        private ConsumerConnector consumer;
        private KafkaStream<byte[], byte[]> stream;

        public ConsumerThread(KafkaStream<byte[], byte[]> stream, ConsumerConnector consumer){
            this.stream=stream;
            this.consumer=consumer;
        }

        @Override
        public void run() {

            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            int i = 0;
            logger.debug("Consumer********************** start.");
            while (it.hasNext() && i < GETMSGS) {
                String log = "";
                MessageAndMetadata<byte[], byte[]> ma = it.next();
                String str = new String(ma.message());
                long partition = ma.partition();
                log += " partition:" + partition;
                long offset = ma.offset();
                log += " offset:" + offset;
                String key = new String(ma.key());
                log += " key:" + key;
                i++;
                logger.debug("Consumer**********************>" + i + log + " message:" + str);
                try {Thread.sleep(SLEEPTIME);} catch (Exception e) {}
            }
            logger.debug("Consumer********************** end.");
            consumer.commitOffsets();
            consumer.shutdown();
        }

    }

    public void consume() {
        long start = System.currentTimeMillis();
        ConsumerConnector consumer = getConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(PARTITIONS));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        ExecutorService executorService = Executors.newFixedThreadPool(PARTITIONS);
        for (KafkaStream<byte[], byte[]> stream : consumerMap.get(TOPIC)) {
            ConsumerThread consumerThread = new ConsumerThread(stream, consumer);
            executorService.execute(consumerThread);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {}
        logger.info("Total time:"+(System.currentTimeMillis() - start));
    }

    public static void main(String[] args) {
        new ConsumerDemo().consume();
    }
}
