package com.ideal.flume.stat;

import java.util.List;

import com.ideal.flume.clients.ZmqClient;

public class ZeromqStatWriter extends StatWriter {
  private final ZmqClient zmqClient;
  private final boolean isReq;

  /**
   *
   * @param zmqUrl pub@tcp://10.4.25.32:8901
   */
  public ZeromqStatWriter(String zmqUrl) {
    int i = zmqUrl.indexOf('@');
    if (i == -1) {
      throw new IllegalArgumentException("error zmq url.");
    }

    String type = zmqUrl.substring(0, i);
    this.isReq = "req".equalsIgnoreCase(type);
    String url = zmqUrl.substring(i + 1);
    this.zmqClient = new ZmqClient(url, type, 1000000);
  }

  @Override
  public void write(List<Stat> stats) {
    for (Stat stat : stats) {
      this.write(stat2Json4Js(stat));
    }
  }

  @Override
  public void write(Stat stat) {
    this.write(stat2Json(stat));
  }

  @Override
  public void write(String stat) {
    zmqClient.send(stat);
    if (isReq) {
      zmqClient.recv();
    }
  }

}
