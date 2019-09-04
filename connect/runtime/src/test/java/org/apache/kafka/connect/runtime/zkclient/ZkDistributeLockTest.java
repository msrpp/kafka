package org.apache.kafka.connect.runtime.zkclient;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author: chenweijie@lvwan.com
 * @Date: 2019-08-31
 * @Description
 */
public class ZkDistributeLockTest {

    @Test
    public void lock() {
        ZkDistributeLock lock = new ZkDistributeLock("127.0.0.1:2181","/user/cwj");
        lock.lock("node1001","empty content");
    }

    @Test
    public void unlock() {
    }
}