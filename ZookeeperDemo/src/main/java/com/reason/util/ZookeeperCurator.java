package com.reason.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.util.List;

public class ZookeeperCurator {

    private static Logger logger = Logger.getLogger(ZookeeperCurator.class);


    private String zookeeperHostPort;

    private CuratorFramework client = null;


    public void init(){
        client = CuratorFrameworkFactory.newClient(zookeeperHostPort, new ExponentialBackoffRetry(10000, 10));
        client.start();
    }


    public static void main(String[] args) throws Exception {

        CuratorFramework client = CuratorFrameworkFactory.newClient("10.16.238.82:2181", new ExponentialBackoffRetry(10000, 10));
        client.start();

        List<String> childrens = client.getChildren().forPath("/");

    }


}
