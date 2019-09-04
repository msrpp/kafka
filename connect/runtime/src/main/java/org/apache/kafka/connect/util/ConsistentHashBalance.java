package org.apache.kafka.connect.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: chenweijie@lvwan.com
 * @Date: 2019-09-03
 * @Description
 */


public class ConsistentHashBalance<T> {


    private static final int PER_VIRTUAL_NUM = 20;
    ReadWriteLock lock = new ReentrantReadWriteLock();

    private final TreeMap<Long, T> virtualNodes = new TreeMap<>();
    private Map<String,Integer> nodes2weight = new ConcurrentHashMap<>();

    private static long bytes2Long(byte[] byteNum,int offset) {
        long num = 0;
        for (int ix = offset; ix < offset+8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    private List<Long> genVitualKeys(String routeValue,int weight){

        List<Long>result = new ArrayList<>();
        for (int i = 0; i< PER_VIRTUAL_NUM *weight/2; i++){
            byte []md5 = Md5Utils.md5(routeValue+String.valueOf(i));
            Long key = bytes2Long(md5,0);
            Long key2 =bytes2Long(md5,8);
            result.add(key);
            result.add(key2);

        }
        return result;
    }

    public void addRouter(T route,int weight){
        String routeValue = route.toString();
        List<Long> keys= genVitualKeys(routeValue,weight);
        lock.writeLock().lock();
        nodes2weight.put(routeValue,weight);
        for (Long key:keys){
            virtualNodes.put(key,route);
        }
        lock.writeLock().unlock();
    }

    public T select(String params){
        byte[] bytes = Md5Utils.md5(params);
        T selected = null;
        Long key1 = bytes2Long(bytes,0);
        Long key2 = bytes2Long(bytes,8);
        Long key = key1^key2;
        lock.readLock().lock();
        SortedMap<Long,T> sorted =  virtualNodes.tailMap(key);
        if (sorted.isEmpty()){
            selected = virtualNodes.firstEntry().getValue();
        }else{
            selected = virtualNodes.get(sorted.firstKey());
        }
        lock.readLock().unlock();
        return selected;
    }

}
