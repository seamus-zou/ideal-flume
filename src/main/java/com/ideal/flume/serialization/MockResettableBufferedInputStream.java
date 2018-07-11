package com.ideal.flume.serialization;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.flume.serialization.LengthMeasurable;
import org.apache.flume.serialization.RemoteMarkable;
import org.apache.flume.serialization.ResettableInputStream;

public class MockResettableBufferedInputStream extends ResettableInputStream
    implements RemoteMarkable, LengthMeasurable {
  private BufferedInputStream in;

  public MockResettableBufferedInputStream(InputStream in, int bufSize) throws IOException {
    if (in instanceof BufferedInputStream) {
      this.in = (BufferedInputStream) in;
    } else {
      this.in = new BufferedInputStream(in, bufSize);
    }
  }

  @Override
  public long length() throws IOException {
    return 0;
  }

  @Override
  public long getMarkPosition() throws IOException {
    return 0;
  }

  @Override
  public void markPosition(long arg0) throws IOException {

  }

  @Override
  public void close() throws IOException {
    this.in.close();
  }

  @Override
  public void mark() throws IOException {

  }

  @Override
  public int read() throws IOException {
    throw new IOException("Unsupported Method.");
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public int readChar() throws IOException {
    throw new IOException("Unsupported Method.");
  }

  @Override
  public void reset() throws IOException {

  }

  @Override
  public void seek(long arg0) throws IOException {

  }

  @Override
  public long tell() throws IOException {
    return 0;
  }

}
