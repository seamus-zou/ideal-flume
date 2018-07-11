package com.ideal.flume.clients;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;

public interface CollectClient {
  boolean isConnected() throws Exception;

  void disconnect() throws Exception;

  List<CollectFile> listFiles(String pathname, CollectFileFilter filter) throws Exception;
  
  List<CollectFile> listFiles(String pathname, CollectFileFilter filter, boolean recursive) throws Exception;

  void deleteFile(String relPath) throws Exception;

  boolean rename(String oldName, String newName) throws Exception;

  InputStream retrieveFileStream(String relPath) throws Exception;

  OutputStream create(String relPath) throws Exception;

  boolean exists(String relPath) throws Exception;

  boolean abort() throws Exception;

  void completePendingCommand() throws Exception;

  boolean mkdirs(String path) throws Exception;
}
