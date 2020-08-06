package com.apache.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {
    private String connectString="hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private  int sessionTimeout=2000;
    private ZooKeeper zkClient;
    //公用获取链接
    @Before
    public void init() throws IOException {
        zkClient= new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                List<String> children= null;
                System.out.println("-----start-----");
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                // 再次启动监听
                try {
                    children = zkClient.getChildren("/",true);
                    for (String child:children){
                        System.out.println(child);
                    }
                    System.out.println("-----end-----");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    //1、创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String path=  zkClient.create("/Amy","ZooKeeperStudy".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }
    //2、获取子节点并监控节点的变化
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children= zkClient.getChildren("/",true);
        for (String child:children){
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }
    //3、判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat=zkClient.exists("/Amy/java",false);
        System.out.println(stat==null?"not exist":"exist");
    }
}
