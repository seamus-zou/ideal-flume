package com.ideal.flume.serialization;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;

public class BufferedBytesDeserializer implements EventDeserializer {
  private final ResettableInputStream in;
  private volatile boolean isOpen;

  public static final int BUFFER_SIZE = 8192;

  BufferedBytesDeserializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.isOpen = true;
  }

  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    byte[] bytes = read();
    if (bytes == null || bytes.length == 0) {
      return null;
    } else {
      return EventBuilder.withBody(bytes);
    }
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  private byte[] read() throws IOException {
    byte[] ret = new byte[BUFFER_SIZE];
    if (-1 == in.read(ret, 0, BUFFER_SIZE)) {
      return null;
    }

    return ret;
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      return new BufferedBytesDeserializer(context, in);
    }

  }


}
