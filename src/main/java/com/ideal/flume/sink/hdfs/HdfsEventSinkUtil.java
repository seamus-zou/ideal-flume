package com.ideal.flume.sink.hdfs;

import com.ideal.flume.tools.RuntimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jred on 2016/12/29.
 */
public class HdfsEventSinkUtil {

    static Logger logger = LoggerFactory.getLogger(HdfsEventSinkUtil.class);

    /**
     * 解析外部的shell命令
     * @param shellStr
     * @param day
     * @param hour
     * @return
     */
    public static String parseShellCmd(String shellStr,String day,String hour){
        if(StringUtils.isNotEmpty(shellStr)) {
            if (StringUtils.isNotEmpty(day)) {
                shellStr = shellStr.replace("${day}", day);
            }
            if (StringUtils.isNotEmpty(hour)) {
                shellStr = shellStr.replace("${hour}", hour);
            }
        }
        return shellStr;
    }

    /**
     * 执行shell命令
     * @param cmd
     * @return
     */
    public static String executeShellCmd(String cmd){
        return RuntimeUtils.exeCmd(cmd);
    }


    /**
     * 执行外部shell命令
     * @param shellStr
     * @param day
     * @param hour
     */
    public static void executeExterShellCmd(String shellStr,String day,String hour){
        if(StringUtils.isNotEmpty(shellStr)){
            String cmd = parseShellCmd(shellStr,day,hour);
            logger.info("start execute cmd:"+cmd);
            String rs = executeShellCmd(cmd);
            logger.info("end execute.the result is:"+rs);

        }
    }

}
