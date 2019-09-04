package org.apache.kafka.connect.runtime.zkclient;


import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Author: chenweijie@lvwan.com
 * @Date: 2019-08-29
 * @Description
 */
public class ZookeeperClient {

    private final CuratorFramework client;

    public ZookeeperClient(String hosts){
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .retryPolicy(new RetryNTimes(3, 1000))
                .connectionTimeoutMs(5000);
        client = builder.build();
        client.start();
    }

    public boolean createEphemeral(String path) {
        return createEphemeral(path,"");
    }

    public void createPersistant(String path){
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);

        }
    }

    public boolean createEphemeral(String path,String data) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path,data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean delete(String path) {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public List<String> list(String path) throws Exception {

        return client.getChildren().forPath(path);
    }

    public Pair<List<String> , Integer> listWithVersion(String path){
        Stat stat = new Stat();
        List<String> children;
        try {
            children = client.getChildren().storingStatIn(stat).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        int version = stat.getCversion();

        return new ImmutablePair<>(children,version);

    }

    public List<String> addListener(String path, CuratorWatcher listener) {
        try {
            return client.getChildren().usingWatcher(listener).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public int getVersion(String node) {
        int version = -1;
        try {
            Stat stat = new Stat();
            client.getData().storingStatIn(stat).forPath(node);
            version = stat.getCversion();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return version;
    }
}
