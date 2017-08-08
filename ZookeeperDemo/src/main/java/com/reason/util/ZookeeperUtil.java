package com.reason.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class ZookeeperUtil implements Watcher{


    private static Logger logger = Logger.getLogger(ZookeeperUtil.class);


    @Value("${kafka.consumer.zk.list}")
    private String zookeeHostPort;

    @Value("${zookeeper.timeout:30000}")
    private String zookeeTimeout;

    private static ZooKeeper zk = null;

    /**
     * 处理当endtime发生变化时，设置标志停止增量同步
     */
    @Override
    public void process(WatchedEvent event) {
        logger.info("********************event**" + event.getType().toString() + "************************");
    }


    /**
     * 创建一个节点
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createNode(String path, String data) throws KeeperException, InterruptedException {
        // 去掉最后一个斜杠
        if(path.lastIndexOf("/") == path.length()-1){
            path = path.substring(0, path.length()-1);
        }
        // 获取父节点
        String parentPath = path.substring(0, path.lastIndexOf("/"));
        if(parentPath.length()==0){
            parentPath = "/";
        }
        // 如果父节点不存在，先创建父节点
        if(!exist(parentPath)){
            createNode(parentPath,"");
        }
        zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    public Stat setDataByPath(String path, String dateStr) throws KeeperException, InterruptedException {
        if(!exist(path)){
            createNode(path, "");
        }
        return zk.setData(path, dateStr.getBytes(), -1);
    }

    public String getDataByPath(String path) throws KeeperException, InterruptedException {
        if(exist(path)){
            byte[] dataBatyes = zk.getData(path, this, null);
            return dataBatyes != null ? new String(dataBatyes) : null;
        }else{
            return null;
        }
    }

    public List<String> getChild(String path)throws KeeperException, InterruptedException {
        List<String> childs = zk.getChildren(path, this);
        return null;
    }

    public boolean deleteNode(String path) throws InterruptedException, KeeperException {
        zk.delete(path, -1);
        return true;
    }

    public boolean exist(String path) throws KeeperException, InterruptedException {
        return null != zk.exists(path, this);
    }


    /**
     * 初始化方法
     */
    public synchronized void init() {
        if (zk == null || !zk.getState().isConnected()) {
            try {
                zk = new ZooKeeper(zookeeHostPort, Integer.parseInt(zookeeTimeout), this);
            } catch (IOException e) {
                logger.error("Create Zookeeper connection failed.", e);
            }
        }
    }


    /**
     * 心跳
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean ping() throws KeeperException, InterruptedException {
        return exist("/");
    }




    @PostConstruct
    public void watchZkStatue() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (!ping()) {
                        init();
                    }
                } catch (Exception e) {
                    logger.warn("Connection zookeeper server failed.",e);
                    init();
                }
            }
        }, 0, 30000);
    }
}
