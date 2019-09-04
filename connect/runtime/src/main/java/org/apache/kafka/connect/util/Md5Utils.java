package org.apache.kafka.connect.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @Author: chenweijie
 * @Date: 2019-09-02
 * @Description
 */
public class Md5Utils {

    private Md5Utils(){

    }

    public static byte[] md5(String input){
        byte[] bytes;
        try {
            bytes = input.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return md5(bytes);
    }

    public static byte[] md5(byte[] input){

        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        md5.reset();
        md5.update(input);
        return md5.digest();
    }
}