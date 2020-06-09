package com.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Auther wu
 * @Date 2019/6/28  20:07
 */

//ZooKeeper的API操作
public class Code_01_ZooKeeperClient {

    private static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static int SessionTimeout = 2000;
    private static ZooKeeper zkClient = null;

    //0. 获取ZooKeeper集群客户端
    @Before
    public void before() throws Exception {

        //Watcher接口的匿名实现类，
        zkClient = new ZooKeeper(connectString, SessionTimeout, new Watcher() {

            // 重写process()方法，收到事件通知后的回调函数（用户的业务逻辑）
            public void process(WatchedEvent event) {
                System.out.println("==默认的回调函数==");
                //System.out.println(event.getType()+"\t"+event.getPath());
            }
        });
    }

    //1. 创建子节点
    @Test
    public void create() throws Exception {

        //参数1，要创建的节点的路径
        //参数2，节点数据
        //参数3，节点的权限
        //参数4，节点的类型(persistent,ephemeral)
        zkClient.create("/zkAPI", "haha".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //2. 获取节点的数据
    @Test
    public void getData() throws Exception {

        Stat stat = new Stat();
        byte[] data = zkClient.getData("/test", true, stat);
        System.out.println(new String(data));
        System.out.println(stat.getCzxid());
    }

    //3. 获取子节点并监听节点变化
    @Test
    public void listAll() throws Exception {

        List<String> children = zkClient.getChildren("/", new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("==自定义的回调函数==");
            }
        });

        for (String child : children) {
            System.out.println(child);
        }

        //如果希望监听子节点变化，则会话不能关闭。且监听只会监听一次
        //Thread.sleep(Long.MAX_VALUE);
    }

    //4. 删除节点
    @Test
    public void delete() throws Exception {
        Stat stat = new Stat();
        int version = stat.getVersion();
        zkClient.delete("/zkAPI", version);
    }

    //5. 判断ZNode节点，是否存在
    @Test
    public void exists() throws Exception {
        Stat stat = zkClient.exists("/test", true);
        System.out.println(stat != null ? "节点存在" + stat.getVersion() : "不存在");
        System.out.println(stat.getDataLength());
    }

    //6. 重新设置节点数据
    @Test
    public void set() throws Exception {
        Stat stat = new Stat();
        int version = stat.getVersion();

        //数据版本要一致
        //Stat stat1 = zkClient.setData("/test", "哈哈，你被我修改了".getBytes(), version);
        Stat stat1 = zkClient.setData("/test", "哈哈，你被我修改了".getBytes(), 1);
        System.out.println(stat1);
    }


    //7. 循环监听节点变化
    public void getChildren() {
        try {

            List<String> children = zkClient.getChildren("/", new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("这是我自定义的回调函数");
                    getChildren();
                }
            });

            System.out.println("========================");
            for (String child : children) {
                System.out.println("----" + child + "----");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws Exception {
        getChildren();
        Thread.sleep(Long.MAX_VALUE);
    }


    @After
    public void after() throws Exception {
        zkClient.close();
    }
}
