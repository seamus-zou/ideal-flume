package com.ideal.flume.sink.hdfs;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * 按时间分组统计hdfs写入的数据
 * Created by jred on 2017/1/12.
 */
public class HdfsSinkTimeGroupCounter {
    /**
     * hadoop用户名
     */
    private  String hadoopUserName;
    /**
     * hive表名称，包含在hdfs路径中
     */
    private  String hiveTableName;
    /**
     * 数据行数
     */
    private AtomicLong counts = new AtomicLong(0);

    public HdfsSinkTimeGroupCounter() {

    }

    public HdfsSinkTimeGroupCounter(String hadoopUserName, String hiveTableName) {
        this.hadoopUserName = hadoopUserName;
        this.hiveTableName = hiveTableName;
    }

    /**
     * 递增方法
     * @return
     */
    public long increment(){
        return counts.getAndIncrement();
    }


    /**
     * 增加方法
     * @param value
     * @return
     */
    public long add(long value){
        return counts.getAndAdd(value);
    }

    /**
     * 设置方法
     * @param value
     */
    public void set(long value){
         counts.set(value);
    }

    public long getCountValue(){
        return counts.longValue();
    }

    public String getHadoopUserName() {
        return hadoopUserName;
    }

    public void setHadoopUserName(String hadoopUserName) {
        this.hadoopUserName = hadoopUserName;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    public void setHiveTableName(String hiveTableName) {
        this.hiveTableName = hiveTableName;
    }

    /**
     * 重置值
     */
    public void resetVal(){
        counts.set(0);
    }

}


