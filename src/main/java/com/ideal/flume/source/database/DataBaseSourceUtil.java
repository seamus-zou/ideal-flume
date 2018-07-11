package com.ideal.flume.source.database;

import com.ideal.flume.tools.jdbc.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库数据源工具类
 * Created by jred on 2016/11/14.
 */
public class DataBaseSourceUtil {


    /**
     * MYSQL分页语句
     */
    private  static final String MYSQL_PAGE_SQL = "select * from ($1) t limit $2,$3";
    /**
     * ORACLE分页语句
     */
    private  static final String ORACLE_PAGE_SQL = "select * from (select t.* ,ROWNUM RN from ($1)t where ROWNUM <= $2 )" +
            " where RN >= $3";

    /**
     * SQLSERVER 2005及以上版本分页语句
     */
    private static final String  SQLSERVER_PAGE_SQL = " select  TOP $2 FROM " +
            "         (" +
            "         SELECT ROW_NUMBER() OVER() AS RowNumber,* FROM ($1) t" +
            "         ) A" +
            " WHERE RowNumber > $3";






    /**
     * 设置database属性
     * @param context
     * @param properties
     */
    public static void setDbProperties(Context context, Properties properties){
           Map<String,String>  map =context.getSubProperties(DataBaseSourceConstants.PROPERTY_PREFIX);
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
     * 获取数据的总大小
     * @param querySql
     * @return
     */
    public static  long  getSize(DataSource dataSource,String querySql){
        String countSql = "select count(0) from (${1}) t".replace("${1}",querySql);
        Object[] objects = DbUtils.queryForArray(new QueryRunner(dataSource),countSql,null);
        return Long.valueOf(objects[0].toString());
    }

    /**
     * 计算分页数
     * @param totalSize
     * @param pageSize
     * @return
     */
    public static int computePage(long totalSize,int pageSize){
        return (int)(totalSize / pageSize  +1 );

    }


    /**
     * 分页查询数据
     * @param dbType
     * @param querySql
     * @param currentPage
     * @param pageSize
     * @return
     */
    public static List<Map<String,Object>> getListByPage(DataSource dataSource,String dbType,String querySql,int currentPage,int pageSize ){
        List<Map<String,Object>> list = null;
        int currentRow = (currentPage-1)*pageSize;
        String sql = "";
        if("MYSQL".equals(dbType)){
             sql = MYSQL_PAGE_SQL.replace("$1",querySql).replace("$2",currentRow+"").replace("$3",pageSize+"");
        }else if("ORACLE".equals(dbType)){
             sql = ORACLE_PAGE_SQL.replace("$1",querySql).replace("$2",(currentRow+pageSize)+"").replace("$3",currentRow+"");
        }else if("SQLSERVER2005".equals(dbType)){
             sql = SQLSERVER_PAGE_SQL.replace("$1",querySql).replace("$2",pageSize+"").replace("$3",currentRow+"");
        }
        if(StringUtils.isNoneEmpty(sql)){
            list = DbUtils.queryForMapList(new QueryRunner(dataSource),sql,null);
        }
        System.out.println("sql = "+sql);
        return  list;

    }

    /**
     * 查询所有的列表数据
     * @param querySql
     * @return
     */
    public static List<Map<String,Object>> getList(DataSource dataSource,String querySql){
        return DbUtils.queryForMapList(new QueryRunner(dataSource),querySql,null);
    }

}
