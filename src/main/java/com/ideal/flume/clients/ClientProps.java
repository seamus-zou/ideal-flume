package com.ideal.flume.clients;

import org.apache.hadoop.conf.Configuration;

import com.ideal.flume.enums.ClientType;

public class ClientProps {
    private final String name;
    private ClientType type;
    private String ip;
    private int port;
    private String userName;
    private String password;
    private String workingDirectory;
    private Integer defaultTimeout;
    private Integer connectTimeout;
    private Integer dataTimeout;
    private Integer soTimeout;
    private boolean passiveMode;
    private boolean remoteverification = true;
    private String controlEncoding;
    private Configuration hdfsConfig;
    private String ticketCache;
    private String keytab;
    private String principal;

    public ClientProps(String name, ClientType type) {
        this.name = name;
        this.type = type;
    }

    public ClientType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public void setType(ClientType type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public Integer getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Integer defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Integer getDataTimeout() {
        return dataTimeout;
    }

    public void setDataTimeout(Integer dataTimeout) {
        this.dataTimeout = dataTimeout;
    }

    public Integer getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(Integer soTimeout) {
        this.soTimeout = soTimeout;
    }

    public boolean isPassiveMode() {
        return passiveMode;
    }

    public void setPassiveMode(boolean passiveMode) {
        this.passiveMode = passiveMode;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public Configuration getHdfsConfig() {
        return hdfsConfig;
    }

    public void setHdfsConfig(Configuration hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    public String getTicketCache() {
        return ticketCache;
    }

    public void setTicketCache(String ticketCache) {
        this.ticketCache = ticketCache;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public boolean isRemoteverification() {
        return remoteverification;
    }

    public void setRemoteverification(boolean remoteverification) {
        this.remoteverification = remoteverification;
    }

}
