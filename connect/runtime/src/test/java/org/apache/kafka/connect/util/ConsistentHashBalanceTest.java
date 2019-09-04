package org.apache.kafka.connect.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @Author: chenweijie
 * @Date: 2019-09-03
 * @Description
 */
public class ConsistentHashBalanceTest {

    @Test
    public void testWeight(){
        ConsistentHashBalance<Integer> consistentHashBalance = new ConsistentHashBalance<>();
        consistentHashBalance.addRouter(1,1);
        consistentHashBalance.addRouter(2,1);
        consistentHashBalance.addRouter(3,2);
        consistentHashBalance.addRouter(4,6);
        Map<Integer,Integer> counter = new HashMap<>();
        counter.put(1,0);
        counter.put(2,0);
        counter.put(3,0);
        counter.put(4,0);
        for (int i = 1;i<1000000;i++){
            Integer c =counter.get(consistentHashBalance.select(String.valueOf(i)));
            c++;
            counter.put(consistentHashBalance.select(String.valueOf(i)),c);
        }
        Assert.assertTrue(80000<counter.get(1) && counter.get(1)<120000);

        Assert.assertTrue(80000<counter.get(2) && counter.get(2)<120000);

        Assert.assertTrue(80000*2<counter.get(3) && counter.get(3)<120000*2);

        Assert.assertTrue(80000*6<counter.get(4) && counter.get(4)<120000*6);
    }
}