package org.apache.kafka.connect.runtime.zkclient;

/**
 * @Author: chenweijie@lvwan.com
 * @Date: 2019-08-31
 * @Description
 */
public interface Lock {

    void lock(String node,String data);

    void unlock(String node);

}
