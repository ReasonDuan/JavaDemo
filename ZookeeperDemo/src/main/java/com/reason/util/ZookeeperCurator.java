package com.reason.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZookeeperCurator {

    private static Logger logger = Logger.getLogger(ZookeeperCurator.class);

    @Value("${zookeeper.hosts}")
    private String zookeeperHostPort;

    private CuratorFramework client = null;


    public void init(){
        client = CuratorFrameworkFactory.newClient(zookeeperHostPort, new ExponentialBackoffRetry(10000, 10));
        client.start();
    }

    public void createPath(String path) throws Exception{
        if(!exist(path)){
            client.create().forPath(path);
        }
    }

    public void createPath(String path, String data) throws Exception{
        if(!exist(path)){
            client.create().forPath(path, data.getBytes());
        }
    }


    public String getData(String path) throws Exception{
        byte[] val = null;
        if(exist(path)){
            val = client.getData().forPath(path);
        }
        return val == null ? null :new String(val);
    }

    public void setData(String path, String data) throws Exception{
        if(exist(path)){
            client.setData().forPath(path, data.getBytes());
        }else{
            client.create().forPath(path, data.getBytes());
        }
    }

    public boolean exist(String path) throws Exception{
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }


    public InterProcessMutex getLock(String path){
        InterProcessMutex lock = new InterProcessMutex(client, path);
        return lock;
    }

    public static void test() throws Exception{

        String path = "/apps/wishlist_reason/reason";

        CuratorFramework client = CuratorFrameworkFactory.newClient("10.16.238.82:2181", new ExponentialBackoffRetry(10000, 10));
        client.start();

//        List<String> childrens = client.getChildren().forPath(path);
//        System.out.println("ChildList:");
//        childrens.forEach(a->{System.out.println(a);});

        byte[] val = client.getData().forPath(path);
        System.out.println("Data:"+new String(val));

        client.create().forPath(path);
//        client.setData().forPath(path, "123456".getBytes());
//        byte[] val = client.getData().forPath(path);
//        System.out.println("Data:"+new String(val));
        Stat stat = client.checkExists().forPath(path);
        System.out.println(stat);
    }


    public static void main(String[] args) throws Exception {


        testLock();

    }


    public static void testLock(){
        int qty = 10;

        ExecutorService service = Executors.newFixedThreadPool(qty);
        for(int i=0; i<qty; i++){
            service.submit(new Queuer());
        }
        service.shutdown();
    }


    public static class Queuer implements Runnable{

        private static int queue = 0;

        private CuratorFramework client = null;
        private InterProcessMutex lock = null;
        private String lockPath = "/apps/wishlist_reason/reason";
        Queuer(){
            client = CuratorFrameworkFactory.newClient("10.16.238.82:2181", new ExponentialBackoffRetry(10000, 10));
            client.start();
            lock = new InterProcessMutex(client, lockPath);
        }

        @Override
        public void run() {
            int n = 20;
            while ( n-- > 0){
                try {
                    //lock.acquire(1, TimeUnit.SECONDS);
                    lock.acquire();
                    if(lock.isAcquiredInThisProcess()){
                        queue++;
                        logger.info("---->"+Thread.currentThread().getName()+": acquire lock "+queue);
                        //Thread.sleep(100);
                    }else{
                        //logger.info(Thread.currentThread().getName()+": could not acquire lock "+n);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        //e.printStackTrace();
                    }
                }
            }

        }
    }
}
