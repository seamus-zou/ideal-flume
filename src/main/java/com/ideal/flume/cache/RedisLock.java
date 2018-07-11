package com.ideal.flume.cache;

import redis.clients.jedis.JedisCommands;

import java.util.Random;

/**
 *
 * Redis分布式锁实现
 * Created by jred on 2017/1/16.
 */
public class RedisLock {

    /**
     * 加锁标志
     */
    public static final String LOCKED = "true";
    /**
     * 毫秒和毫微秒的转换单位
     */
    public static final long CONVERT_UNIT = 1000*1000L;
    /**
     * 默认加锁超时时间(毫秒)
     */
    public static final int DEFAULT_TIME_OUT = 1000;

    public static final Random random = new Random();
    /**
     * 锁的过期时间（秒）
     */
    public static final int EXPIRE_TIME = 60;
    /**
     * 是否锁定
     */
    private boolean isLocked = false;
    /**
     * 锁的key。由操作key和key后缀<code>KEY_SUFFIX</code>组成
     */
    private String key;
    /**
     * 锁的key后缀
     */
    public static final String KEY_SUFFIX = "_lock";

    private JedisCommands jedisCommands;


    public RedisLock(String key, JedisCommands jedisCommands) {
        this.key = key+KEY_SUFFIX;
        this.jedisCommands = jedisCommands;
    }


    /**
     * 加锁
     * @param lockTimeOut 加锁超时时间
     * @param expireTime 锁的过期时间
     * @return
     */
    public boolean lock(int lockTimeOut,int expireTime){
        long nanoTime = System.nanoTime();
        lockTimeOut *= CONVERT_UNIT;
        try{
            while ((System.nanoTime() - nanoTime < lockTimeOut)){
                if(jedisCommands.setnx(key,LOCKED)==1){
                    jedisCommands.expire(key,expireTime);
                    this.isLocked = true;
                    return true;
                }
                // 短暂休眠，避免出现活锁
                Thread.sleep(3, random.nextInt(500));
            }

        }catch (Exception e){
            new RuntimeException("lock error!"+e.getMessage(),e);
        }
        return false;
    }

    /**
     * 加锁方法
     * @param expireTime
     * @return
     */
    public boolean lock(int expireTime){
        return lock(DEFAULT_TIME_OUT,expireTime);
    }

    /**
     * 加锁方法
     * @return
     */
    public boolean lock(){
        return lock(DEFAULT_TIME_OUT,EXPIRE_TIME);
    }


    /**
     * 解锁方法
     * @return
     */
    public void unLock(){
        if(isLocked){
            jedisCommands.del(key);
        }
    }


}
