package com.atguigu.zk;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * zookeeper的入门程序
 */
public class HelloZK {

    private static Logger logger = LogManager.getLogger(HelloZK.class);

    private static final String CONNECTSTRING = "192.168.199.129:2181";
    private static int SESSION_TIMEOUT = 30 * 1000;
    private static String PATH = "/atguigu";

    private String oldVal;
    private ZooKeeper zooKeeper;

    public ZooKeeper start() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //检测指定路径下的子节点变化情况
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && watchedEvent.getPath() == PATH) {
                    printChildNodes(PATH);
                } else {
                    printChildNodes(PATH);
                }

            }
        });
    }

    //打印子节点
    public void printChildNodes(String path) {

        try {
            if (zooKeeper.exists(path, false) != null) {
                List<String> children = this.zooKeeper.getChildren(path, true);
                System.out.println("子节点：" + children);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建节点
    public void createZkNode(String path, String value) throws KeeperException, InterruptedException {
        this.zooKeeper.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取节点值
    public String getNodeVal(String path) throws KeeperException, InterruptedException {
        byte[] data = this.zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                printNodeVal(PATH);
            }
        }, new Stat());
        String result = new String(data);
        oldVal = result;//记录第一次的值
        return result;
    }

    public void printNodeVal(String path) {
        String result = null;
        try {
            byte[] nodeVal = this.zooKeeper.getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    printNodeVal(path);
                }
            }, new Stat());
            result = new String(nodeVal);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (oldVal.equals(result)) {
            System.out.println("值未发生改变");

        }else{
            System.out.println("值发生改变  oldVla:" + oldVal + " newVal:" + result);
            oldVal = result;
        }

    }

    //关闭服务
    public void stop() throws InterruptedException {
        if (this.zooKeeper != null) this.zooKeeper.close();
    }


    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public String getOldVal() {
        return oldVal;
    }

    public void setOldVal(String oldVal) {
        this.oldVal = oldVal;
    }

    //监听节点发生变化
    @Test
    public void test() throws Exception {

        HelloZK helloZK = new HelloZK();
        ZooKeeper zooKeeper = helloZK.start();
        helloZK.setZooKeeper(zooKeeper);

        if (zooKeeper.exists(PATH, false) == null) {
            helloZK.createZkNode(PATH, "test");
            String val = helloZK.getNodeVal(PATH);
            logger.info("创建的节点数据为：" + val);
            Thread.sleep(Integer.MAX_VALUE);
        } else {
            logger.info("该节点已存在，创建失败");
        }

    }


    //监听节点的子节点的变化，指的是节点的增删，并不是值的改变
    @Test
    public void test1() throws IOException, InterruptedException {
        HelloZK helloZK = new HelloZK();
        ZooKeeper zooKeeper = helloZK.start();
        helloZK.setZooKeeper(zooKeeper);
        Thread.sleep(Integer.MAX_VALUE);
    }

}
