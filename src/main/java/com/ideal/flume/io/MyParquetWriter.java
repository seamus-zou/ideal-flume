package com.ideal.flume.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class MyParquetWriter {

	public static void parquetWriter(String outPath) throws IOException{
        MessageType schema = MessageTypeParser.parseMessageType(
        		"message Pair {\n"
                		+ " required binary srcip (UTF8);\n"
        				+ " required binary ad (UTF8);\n" 
                		+ " required binary ts (UTF8);\n"
        				+ " required binary url (UTF8);\n" 
                		+ " required binary ref (UTF8);\n"
        				+ " required binary ua (UTF8);\n" 
                		+ " required binary dstip (UTF8);\n"
        				+ " required binary cookie (UTF8);\n" 
                		+ " required binary src_port (UTF8);\n"
//        				+ " required binary datelabel (UTF8);\n" 
//        				+ " required binary loadstamp (UTF8);\n" 
        				+ "}");
        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
       Configuration configuration = new Configuration();
       GroupWriteSupport writeSupport = new GroupWriteSupport();
       writeSupport.setSchema(schema,configuration);
      ParquetWriter<Group> writer = new ParquetWriter<Group>(path,configuration,writeSupport);
      //把本地文件读取进去，用来生成parquet格式文件
//       BufferedReader br =new BufferedReader(new FileReader(new File(inPath)));
//       String line="";
//       Random r=new Random();
      int i =0;
       while(i<100){
//           String[] strs=line.split("\\s+");
//           if(strs.length==2) {
        	   Group group = factory.newGroup()
               		.append("srcip","srcip"+i)
               		.append("ad","ad"+i)
               		.append("ts","ts"+i)
               		.append("url","url"+i)
               		.append("ref","ref"+i)
               		.append("ua","ua"+i)
               		.append("dstip","dstip"+i)
               		.append("cookie","cookie"+i)
               		.append("src_port","src_port"+i);
               writer.write(group);
               i++;
//           }
       }
       System.out.println("write end");
       writer.close();
    }
}
