package com.ideal.flume.tools.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 *  基于apache的dbutils工具类再次封装
 * Created by jred on 16/9/8.
 *
 */
public class DbUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);



    /**
     * 初始化配置
     */
    public static DbBean initConfig(Properties prop){
        DbBean dbBean = new DbBean();
        dbBean.setDriverClass(prop.getProperty("jdbc.driverClass"));
        dbBean.setUrl(prop.getProperty("jdbc.url"));
        dbBean.setUser(prop.getProperty("jdbc.userName"));
        dbBean.setPassword(prop.getProperty("jdbc.password"));
        dbBean.setDataSourceType(prop.getProperty("jdbc.dataSource"));

        if ("c3p0".equals(dbBean.getDataSourceType())) {
            dbBean.setC3p0_acquireIncrement(Integer.valueOf(prop.getProperty("c3p0.acquireIncrement")));
            dbBean.setC3p0_minPoolSize(Integer.valueOf(prop.getProperty("c3p0.minPoolSize")));
            dbBean.setC3p0_maxPoolSize(Integer.valueOf(prop.getProperty("c3p0.maxPoolSize")));
            dbBean.setC3p0_initialPoolSize(Integer.valueOf(prop.getProperty("c3p0.initialPoolSize")));
            dbBean.setC3p0_maxIdleTime(Integer.valueOf(prop.getProperty("c3p0.maxIdleTime")));
//            dbBean.setC3p0_breakAfterAcquireFailure(Boolean.valueOf(prop.getProperty("c3p0.breakAfterAcquireFailure")));
//            dbBean.setC3p0_autoCommitOnClose(Boolean.valueOf(prop.getProperty("c3p0.autoCommitOnClose")));
        }
        return dbBean;
    }




    /**
     * 获取c3p0连接池
     * @param dbBean
     * @return
     */
    public static DataSource getC3p0Datasource(DbBean dbBean){
        System.out.println(dbBean.toString());
        ComboPooledDataSource c3p0DataSource = new ComboPooledDataSource();
        try {
            c3p0DataSource.setDriverClass(dbBean.getDriverClass());
            c3p0DataSource.setUser(dbBean.getUser());
            c3p0DataSource.setPassword(dbBean.getPassword());
            c3p0DataSource.setJdbcUrl(dbBean.getUrl());
            c3p0DataSource.setAcquireIncrement(dbBean.getC3p0_acquireIncrement());
            c3p0DataSource.setMinPoolSize(dbBean.getC3p0_minPoolSize());
            c3p0DataSource.setInitialPoolSize(dbBean.getC3p0_initialPoolSize());
            c3p0DataSource.setMaxIdleTime(dbBean.getC3p0_maxIdleTime());
//            c3p0DataSource.setBreakAfterAcquireFailure(dbBean.getC3p0_breakAfterAcquireFailure());
//            c3p0DataSource.setAutoCommitOnClose(dbBean.getC3p0_autoCommitOnClose());

        } catch (PropertyVetoException e) {
            throw new RuntimeException(e.getMessage(),e);
        }
        return c3p0DataSource;
    }





    /**
     * 打开数据库连接
     * @param dirverClass
     * @param url
     * @param username
     * @param password
     * @return
     */
    public static Connection openConnection(String dirverClass,String url,String username,String password){
        Connection conn = null;
        try {
            Class.forName(dirverClass);
            conn = DriverManager.getConnection(url,username,password);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(),e);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(),e);
        }

        return conn;
    }


    /**
     * 获取数据源连接池配置
     * @param dbBean
     * @return
     */
    public static Connection openConnection(DbBean dbBean){
        Connection conn = null;
        try {
            conn = getC3p0Datasource(dbBean).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;

    }



    /**
     * 关闭数据库连接
     * @param connection
     */
    public static void closeConnection(Connection connection){
        try {
            org.apache.commons.dbutils.DbUtils.close(connection);
        } catch (SQLException e) {
            new RuntimeException(e.getMessage(),e);
        }
    }


    /**
     * 查询返回数组
     * @param runner
     * @param sql
     * @param params
     * @return
     */
    public static Object[] queryForArray(QueryRunner runner,String sql,Object... params){
        Object[] result = null;

        try {
            result = runner.query(sql,new ArrayHandler(),params);

        } catch (SQLException e) {
            LOGGER.error("query result for array is error",e);
            throw new RuntimeException(e.getMessage(),e);
        }
        return result;
    }


    /**
     * 查询结果返回List
     * @param runner
     * @param sql
     * @param params
     * @return
     */
    public static List<Object[]> queryForList(QueryRunner runner, String sql, Object... params) {
        List<Object[]> result = null;
        try {
            result = runner.query(sql, new ArrayListHandler(), params);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }


    /**
     * 查询结果返回Map
     * @param runner
     * @param sql
     * @param params
     * @return
     */
    public static Map<String, Object> queryForMap(QueryRunner runner, String sql, Object... params) {
        Map<String, Object> result = null;
        try {
            result = runner.query(sql, new MapHandler(), params);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 查询返回List<Map>
     * @param runner
     * @param sql
     * @param params
     * @return
     */
    public static List<Map<String, Object>> queryForMapList(QueryRunner runner, String sql, Object... params) {
        List<Map<String, Object>> result = null;
        try {
            result = runner.query(sql, new MapListHandler(), params);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }


    /**
     * 查询返回自定义Bean
     * @param runner
     * @param cls
     * @param map
     * @param sql
     * @param params
     * @param <T>
     * @return
     */
    public static <T> T queryForBean(QueryRunner runner, Class<T> cls, Map<String, String> map, String sql, Object... params) {
        T result = null;
        try {
            if (MapUtils.isNotEmpty(map)) {
                result = runner.query(sql, new BeanHandler<T>(cls, new BasicRowProcessor(new BeanProcessor(map))), params);
            } else {
                result = runner.query(sql, new BeanHandler<T>(cls,new BasicRowProcessor(new GenBeanProcess())), params);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }


    /**
     * 查询返回List<T>
     * @param runner
     * @param cls
     * @param map
     * @param sql
     * @param params
     * @param <T>
     * @return
     */
    public static <T> List<T> queryForBeanList(QueryRunner runner, Class<T> cls, Map<String, String> map, String sql, Object... params) {
        List<T> result = null;
        try {
            if (MapUtils.isNotEmpty(map)) {
                result = runner.query(sql, new BeanListHandler<T>(cls, new BasicRowProcessor(new BeanProcessor(map))), params);
            } else {
                result = runner.query(sql, new BeanListHandler<T>(cls,new BasicRowProcessor(new GenBeanProcess())), params);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 查询指定单个列的值(返回单条)
     * @param runner
     * @param column
     * @param sql
     * @param params
     * @return
     */
    public static Object queryColumn(QueryRunner runner, String column, String sql, Object... params) {
        Object result = null;
        try {
            result = runner.query(sql, new ScalarHandler<Object>(column), params);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 查询返回指定列的值(返回多条)
     * @param runner
     * @param column
     * @param sql
     * @param params
     * @param <T>
     * @return
     */
    public static <T> List<T> queryColumnList(QueryRunner runner, String column, String sql, Object... params) {
        List<T> result = null;
        try {
            result = runner.query(sql, new ColumnListHandler<T>(column), params);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }


    /**
     * 数据更改操作(返回受影响的行数)
     * @param runner
     * @param conn
     * @param sql
     * @param params
     * @return
     */
    public static int update(QueryRunner runner, Connection conn, String sql, Object... params) {
        int result = 0;
        try {
            if (conn != null) {
                result = runner.update(conn, sql, params);
            } else {
                result = runner.update(sql, params);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 批量更新数据
     * @param runner
     * @param conn
     * @param sql
     * @param params
     * @return
     */
    public static int[] batchUpdate(QueryRunner runner, Connection conn, String sql,Object[][] params){
        int[] result;
        try {
            if(conn !=null){
                result = runner.batch(conn,sql,params);
            }else{
                result = runner.batch(sql,params);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;

    }







}
