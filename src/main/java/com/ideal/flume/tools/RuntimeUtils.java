package com.ideal.flume.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Java Runtime工具类
 * Created by jred on 2016/12/29.
 */
public class RuntimeUtils {

    /**
     * 执行命令语句
     * @param cmd
     * @return
     */
    public static String exeCmd(String cmd){
        String result = "";
        String[] cmds = {"/bin/sh","-c",cmd};
        try {
            Process process = Runtime.getRuntime().exec(cmds);
            InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            while ((line = bufferedReader.readLine())!=null){
                result += line;
            }
            InputStreamReader errorInputStream = new InputStreamReader(process.getErrorStream());
            BufferedReader errorBufferedReader = new BufferedReader(errorInputStream);
            String eline = "";
            while ((eline = errorBufferedReader.readLine())!=null){
                System.out.println(eline);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args){
       System.out.println(exeCmd("hive -e \"use oidd;alter table oidd_static  add if not exists PARTITION (datelabel='${day}') location '${day}';\""));
    }

}
