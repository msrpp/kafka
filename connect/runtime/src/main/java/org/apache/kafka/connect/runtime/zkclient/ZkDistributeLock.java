package org.apache.kafka.connect.runtime.zkclient;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: chenweijie
 * @Date: 2019-08-30
 * @Description  non-reentrant
 */
public class ZkDistributeLock implements Lock, java.io.Serializable {

    private static final Logger log = LoggerFactory.getLogger(ZkDistributeLock.class);

    private final ZookeeperClient zkClient;
    private String nodeDir;

    private volatile int version = -1;

    private CuratorWatcher watcher;

    ConcurrentHashMap<String , CreatePathEvent> waiters = new ConcurrentHashMap<>();


    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);


    private static class CreatePathEvent {
        private final CountDownLatch semaphore;
        private final String data;
        public CreatePathEvent(String data) {
            semaphore = new CountDownLatch(1);
            this.data = data;
        }
    }

    public ZkDistributeLock(String zkHosts , final String parentPath){
        this.nodeDir = parentPath;
        zkClient = new ZookeeperClient(zkHosts);
        zkClient.createPersistant(parentPath);

        watcher = new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                    Pair<List<String> ,Integer> childrenWithVersion = zkClient.listWithVersion(nodeDir);
                    List<String> children = childrenWithVersion.getLeft();
                    version = childrenWithVersion.getRight();
                    for (String node : waiters.keySet()){
                        if (!children.contains(node)){
                            CreatePathEvent createPathEvent = waiters.get(node);
                            if (tryLock(nodePath(node),createPathEvent.data)){
                                waiters.remove(node);
                                createPathEvent.semaphore.countDown();
                            }
                        }
                    }


                }
                zkClient.addListener(parentPath,this);
            }
        };

        zkClient.addListener(parentPath, watcher);

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // we should add an addition listener periodly in case NodeChildrenChanged happens before we setting another listener on it
                int curVersion = zkClient.getVersion(nodeDir);
                if (version != curVersion){
                    version = curVersion;
                    zkClient.addListener(parentPath,watcher);

                }


            }
        },5000,10000,TimeUnit.MILLISECONDS);
    }
    private boolean tryLock(String path, String data){
        return zkClient.createEphemeral(path,data);
    }



    public void lock(String node,String data) {

        if (tryLock(nodePath(node),data)){
            return;
        }
        CreatePathEvent event = new CreatePathEvent(data);
        waiters.put(node,event);
        boolean interrupt = false;
        while (true){
            try {
                if(event.semaphore.await(10000,TimeUnit.MILLISECONDS)){
                    break;
                }else{
                    log.warn("cannot get node {} in 10 secends",node);
                }
            } catch (InterruptedException e) {
                interrupt = true;
            }

        }
        if (interrupt){
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void unlock(String node) {
        zkClient.delete(nodePath(node));
    }

    public String nodePath(String node){
        return nodeDir + "/" + node;
    }


}
