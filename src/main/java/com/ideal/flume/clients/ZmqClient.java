package com.ideal.flume.clients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class ZmqClient {
  private final static Logger logger = LoggerFactory.getLogger(ZmqClient.class);

  private final static long RECV_HWM = 100000L;

  private final ZMQ.Context context;
  private final ZMQ.Socket socket;

  public ZmqClient(String url, String type, long HWM) {
    this(1, url, type, HWM, new byte[] {});
  }

  public ZmqClient(String url, String type) {
    this(1, url, type, RECV_HWM, new byte[] {});
  }

  public ZmqClient(int ioThread, String url, String type, long HWM, byte[] subscribe) {
    // bind zmq
    context = ZMQ.context(ioThread);
    if ("pub".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.PUB);
    } else if ("sub".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.SUB);
      socket.subscribe(subscribe);
    } else if ("req".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.REQ);
    } else if ("rep".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.REP);
    } else if ("pull".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.PULL);
    } else if ("push".equalsIgnoreCase(type)) {
      socket = context.socket(ZMQ.PUSH);
    } else {
      throw new IllegalArgumentException("unsupported zmq type");
    }
    try {
      if (HWM > 0)
        socket.setRcvHWM(HWM);
      socket.connect(url);
    } catch (ZMQException e) {
      throw new RuntimeException("can not bind zmq url: " + url, e);
    }
  }

  public synchronized byte[] recv() {
    try {
      return socket.recv(0);
    } catch (ZMQException e) {
      throw new RuntimeException("receive zmq message error.", e);
    } catch (Throwable e) {
      logger.error("", e);
    }
    return null;
  }

  public synchronized boolean send(String msg) {
    try {
      return socket.send(msg);
    } catch (ZMQException e) {
      throw new RuntimeException("send zmq message error.", e);
    } catch (Throwable e) {
      logger.error("", e);
    }
    return false;
  }

  public synchronized boolean send(byte[] msg, int offset, int len) {
    try {
      return socket.send(msg, offset, len, 0);
    } catch (ZMQException e) {
      throw new RuntimeException("send zmq message error.", e);
    } catch (Throwable e) {
      logger.error("", e);
    }
    return false;
  }

  public synchronized void close() {
    socket.close();
    context.term();
  }
}
