package com.ideal.flume.io;

import com.ideal.flume.clients.CollectClient;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class MyYeeFileWriter implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(MyYeeFileWriter.class);

  private OutputStream outputStream;
  private CollectClient fileClient;

  private String serializerType;
  private Context serializerContext;
//  private EventSerializer serializer;
  private MyEventSerializer serializer;
  

  private Compressor compressor;

  /**
   * 构造器.
   * 
   * @param context 配置参数
   * @param fileSystem 文件系统
   */
  public MyYeeFileWriter(Context context, CollectClient fileClient) {
    this.fileClient = fileClient;

    serializerType = context.getString("serializer", "TEXT");
    serializerContext = new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
    logger.info("Serializer = " + serializerType);
  }

  /**
   * 打开普通文件.
   * 
   * @param filePath 绝对路径的文件名
   * @throws Exception 执行失败时抛出
   */
  public void open(String filePath) throws Exception {
    open(filePath, null);
  }

  /**
   * 打开压缩文件.
   * 
   * @param filePath 绝对路径的文件名
   * @param codec 压缩方式
   * @throws Exception 执行失败时抛出
   */
  public void open(String filePath, CompressionCodec codec) throws Exception {
    OutputStream fsOutputStream = fileClient.create(filePath);

    if (null == codec) {
      outputStream = fsOutputStream;
    } else {
      if (null == compressor) {
        compressor = CodecPool.getCompressor(codec);
      }
      outputStream = codec.createOutputStream(fsOutputStream, compressor);
    }

//    serializer =EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
//    String serializerType =  MyEventSerializer.Builder.class.getName();
//    serializer =(MyEventSerializer) EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);;
    serializer = (MyEventSerializer) new MyEventSerializer.Builder().build(serializerContext, outputStream);
    serializer.afterCreate();
  }

  public void append(Event event) throws Exception {
    serializer.write(event);
  }

  public void flush() throws Exception {
    serializer.flush();
  }

  @Override
  public void close() throws IOException {
    serializer.flush();
    serializer.beforeClose();

    outputStream.close();

    if (null != compressor) {
      CodecPool.returnCompressor(compressor);
      compressor = null;
    }
  }

}
