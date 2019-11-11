package org.apache.kafka.connect.runtime.rest.entities;

import java.util.Map;

/**
 * @Author: chenweijie@lvwan.com
 * @Date: 2019-11-08
 * @Description
 */
public class OffsetInfo {

    private Map<String,Object> key;
    private Map<String,Object> value;

    public Map<String, Object> getKey() {
        return key;
    }

    public void setKey(Map<String, Object> key) {
        this.key = key;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

}
