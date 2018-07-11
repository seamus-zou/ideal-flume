package com.ideal.flume.sink.database;

/**
 *
 * 数据库sink常量
 * Created by jred on 2016/12/25.
 */
public class DataBaseSinkConstants {

    /**
     * 属性前缀
     */
    public static final String PROPERTY_PREFIX = "db.";

    /**
     * 数据库字符编码
     */
    public static final String DB_CHARSET = "dbCharset";
    /**
     * 批量提交大小
     */
    public static final String BATCH_SIZE = "batchSize";
    /**
     * 等待提交时间
     */
    public static final String TIMEUPPERLIMIT = "timeUpperLimit";

    /**
     * 数据库表名
     */
    public static final String DB_TABLE_NAME = "dbTableName";
    /**
     * 内容分割字符串
     */
    public static final String SPLIT_STR = "splitStr";


}
