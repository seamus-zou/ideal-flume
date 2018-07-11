package com.ideal.flume.sink.database;

import com.ideal.flume.source.database.DataBaseSourceConstants;
import com.ideal.flume.tools.jdbc.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flume.Context;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jred on 2016/12/25.
 */
public class DataBaseSinkUtil {

    private static String SPACE = "SPACE";

    /**
     * 设置database属性
     * @param context
     * @param properties
     */
    public static void setDbProperties(Context context, Properties properties){
        Map<String,String> map =context.getSubProperties(DataBaseSourceConstants.PROPERTY_PREFIX);
        for(Map.Entry<String,String> entry : map.entrySet()){
            properties.put(entry.getKey(),entry.getValue());
        }
    }

    /**
     * 获取数据源
     * @param properties
     * @return
     */
    public static DataSource getDataSource(Properties properties){
        return  DbUtils.getC3p0Datasource(DbUtils.initConfig(properties));
    }

    /**
     * 获取插入的sql语句
     * @param dbTableName
     * @param colsNum
     * @return
     */
    public static String getInsertSql(String dbTableName,int colsNum){
        String sql = "insert into  "+dbTableName;
        String valueStr = "";
        for(int i=0;i<colsNum;i++){
            if(i<colsNum-1)
                valueStr += "?,";
            else
                valueStr += "?";
        }
        sql += " values("+valueStr+")";
        return  sql;
    }

    /**
     * 批量插入数据
     * @param dataSource
     * @param sql
     * @param params
     * @return
     */
    public static int[] batchInsert(DataSource dataSource,String sql,Object[][] params){
       return DbUtils.batchUpdate(new QueryRunner(dataSource),null,sql,params);
    }

    /**
     * 处理空格类型的分割字符串
     * @param str
     * @return
     */
    public static String formartSpace(String str){
         if(str.startsWith(SPACE)){
             Integer num = Integer.valueOf(str.replace(SPACE,""));
             String s = "";
             for(int i=0;i<num;i++){
                 s += " ";
             }
             return s;
         }
         return str;
    }
}
