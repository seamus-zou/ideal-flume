package com.ideal.flume.source.database;

/**
 * 数据库数据源常量类
 * Created by jred on 2016/11/14.
 */
public class DataBaseSourceConstants {

    /**
     * 属性前缀
     */
    public static final String PROPERTY_PREFIX = "db.";
    /**
     * 批次提交数量
     */
    public static final String BATCH_UPPER_LIMIT = "batchUpperLimit";
    /**
     * 每次提交最大时间间隔
     */
    public static final String TIME_UPPER_LIMIT = "timeUpperLimit";
    /**
     * 是否分页查询
     */
    public static final String IS_PAGEABLE = "isPageable";
    /**
     * 每页数据大小
     */
    public static final String PAGE_SIZE = "pageSize";
    /**
     * 查询语句
     */
    public static final String QUERY_SQL = "querySql";
    /**
     * 数据更新字段名称
     */
    public static final String UPDATE_FIELD_NAME = "updateFieldName";
    /**
     * 数据库类型
     */
    public static final String DB_TYPE = "dbType";
    /**
     * 数据库字符编码
     */
    public static final String DB_CHARSET = "dbCharset";


    /**
     * 增量列名 
     */
    public static final String INCREMENT_COLUMN = "incrementColumn";
    
    /**
     * 增量类型
     */
    public static final String INCREMENT_TYPE = "incrementType";



}
