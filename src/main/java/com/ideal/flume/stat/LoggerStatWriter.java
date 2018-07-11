package com.ideal.flume.stat;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerStatWriter extends StatWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggerStatWriter.class);

  private static final String LINE_SEPARATOR = System.getenv("line.separator");

  @Override
  public void write(List<Stat> stats) {
    StringBuilder ret = new StringBuilder();
    for (Stat s : stats) {
      ret.append(stat2Json(s)).append(LINE_SEPARATOR);
    }
    this.write(ret.toString());
  }

  @Override
  public void write(Stat stat) {
    this.write(stat2Json(stat));
  }

  @Override
  public void write(String stat) {
    LOGGER.info(stat);
  }

}
