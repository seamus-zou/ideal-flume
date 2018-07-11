package com.ideal.flume.tools.jdbc;

/**
 * 数据库连接bean
 * Created by jred on 16/9/8.
 */
public class DbBean {

    /**
     * 数据库驱动类路径
     */
    private String driverClass;
    /**
     * jdbc url
     */
    private String url;
    /**
     * 数据库用户名
     */
    private String user;
    /**
     * 数据登录密码
     */
    private String password = "";
    /**
     * 数据源类型
     */
    private String dataSourceType = "c3p0";


    /**
     * c3p0数据源参数
     *
     */
    private Integer c3p0_acquireIncrement = 3;

    private Integer c3p0_minPoolSize = 5;

    private Integer c3p0_maxPoolSize = 10;

    private Integer c3p0_initialPoolSize = 5;

    private Integer c3p0_maxIdleTime = 1800;

    private Boolean c3p0_breakAfterAcquireFailure = false;

    private Boolean c3p0_autoCommitOnClose = false;



    public Integer getC3p0_acquireIncrement() {
        return c3p0_acquireIncrement;
    }

    public void setC3p0_acquireIncrement(Integer c3p0_acquireIncrement) {
        this.c3p0_acquireIncrement = c3p0_acquireIncrement;
    }

    public Integer getC3p0_minPoolSize() {
        return c3p0_minPoolSize;
    }

    public void setC3p0_minPoolSize(Integer c3p0_minPoolSize) {
        this.c3p0_minPoolSize = c3p0_minPoolSize;
    }

    public Integer getC3p0_maxPoolSize() {
        return c3p0_maxPoolSize;
    }

    public void setC3p0_maxPoolSize(Integer c3p0_maxPoolSize) {
        this.c3p0_maxPoolSize = c3p0_maxPoolSize;
    }

    public Integer getC3p0_initialPoolSize() {
        return c3p0_initialPoolSize;
    }

    public void setC3p0_initialPoolSize(Integer c3p0_initialPoolSize) {
        this.c3p0_initialPoolSize = c3p0_initialPoolSize;
    }

    public Integer getC3p0_maxIdleTime() {
        return c3p0_maxIdleTime;
    }

    public void setC3p0_maxIdleTime(Integer c3p0_maxIdleTime) {
        this.c3p0_maxIdleTime = c3p0_maxIdleTime;
    }

    public Boolean getC3p0_breakAfterAcquireFailure() {
        return c3p0_breakAfterAcquireFailure;
    }

    public void setC3p0_breakAfterAcquireFailure(Boolean c3p0_breakAfterAcquireFailure) {
        this.c3p0_breakAfterAcquireFailure = c3p0_breakAfterAcquireFailure;
    }

    public Boolean getC3p0_autoCommitOnClose() {
        return c3p0_autoCommitOnClose;
    }

    public void setC3p0_autoCommitOnClose(Boolean c3p0_autoCommitOnClose) {
        this.c3p0_autoCommitOnClose = c3p0_autoCommitOnClose;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    @Override
    public String toString() {
        return "DbBean{" +
                "driverClass='" + driverClass + '\'' +
                ", url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", dataSourceType='" + dataSourceType + '\'' +
                ", c3p0_acquireIncrement=" + c3p0_acquireIncrement +
                ", c3p0_minPoolSize=" + c3p0_minPoolSize +
                ", c3p0_maxPoolSize=" + c3p0_maxPoolSize +
                ", c3p0_initialPoolSize=" + c3p0_initialPoolSize +
                ", c3p0_maxIdleTime=" + c3p0_maxIdleTime +
                ", c3p0_breakAfterAcquireFailure=" + c3p0_breakAfterAcquireFailure +
                ", c3p0_autoCommitOnClose=" + c3p0_autoCommitOnClose +
                '}';
    }
}
