package com.reason.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

public class ZookeeperCurator {

    private static Logger logger = Logger.getLogger(ZookeeperCurator.class);

    @Value("${zookeeper.hosts}")
    private String zookeeperHostPort;

    private CuratorFramework client = null;


    public void init(){
        client = CuratorFrameworkFactory.newClient(zookeeperHostPort, new ExponentialBackoffRetry(10000, 10));
        client.start();
    }


    public String getData(String path) throws Exception{
        byte[] val = client.getData().forPath(path);
        return val == null ? null :new String(val);
    }

    public void setData(String path, String data) throws Exception{
        client.setData().forPath(path, data.getBytes());
    }


    public boolean exist(String path) throws Exception{
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }




    public static void main(String[] args) throws Exception {

        String path = "/apps/wishlist_reason/reason/no";

        CuratorFramework client = CuratorFrameworkFactory.newClient("10.16.238.82:2181", new ExponentialBackoffRetry(10000, 10));
        client.start();

//        List<String> childrens = client.getChildren().forPath(path);
//        System.out.println("ChildList:");
//        childrens.forEach(a->{System.out.println(a);});

//        byte[] val = client.getData().forPath(path);
//        System.out.println("Data:"+new String(val));

//        client.create().forPath(path);
//        client.setData().forPath(path, "123456".getBytes());
//        byte[] val = client.getData().forPath(path);
//        System.out.println("Data:"+new String(val));
        Stat stat = client.checkExists().forPath(path);
        System.out.println(stat);

    }


}
