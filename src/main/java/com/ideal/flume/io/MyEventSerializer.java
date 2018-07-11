package com.ideal.flume.io;

import de.codepfleger.flume.avro.serializer.event.SyslogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import de.codepfleger.flume.parquet.serializer.AbstractParquetSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.source.SyslogParser;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class MyEventSerializer extends AbstractParquetSerializer {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final SyslogParser syslogParser;

    public MyEventSerializer() {
        super(new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(SyslogEvent.class)));
        syslogParser = new SyslogParser();
    }

    @Override
    public void write(Event flumeEvent) throws IOException {
        try {
            String syslogMessage = new String(flumeEvent.getBody());
            Event event = syslogParser.parseMessage(syslogMessage, Charset.defaultCharset(), null);
            Map<String, Object> dataMap = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                if ("timestamp".equals(entry.getKey())) {
                    dataMap.put(entry.getKey(), DATE_FORMAT.format(new Date(Long.parseLong(entry.getValue()))));
                } else {
                    dataMap.put(entry.getKey(), entry.getValue());
                }
            }
            dataMap.put("Message", new String(event.getBody()));

            SyslogEvent syslogEvent = new SyslogEvent();
            AbstractReflectionAvroEventSerializer.setFieldsAndRemove(syslogEvent, dataMap);
            syslogEvent.dynamic.putAll(dataMap);

            GenericData.Record record = new GenericData.Record(getSchema());
           /* record.put("Severity", syslogEvent.Severity);
            record.put("Facility", syslogEvent.Facility);
            record.put("host", syslogEvent.host);
            record.put("timestamp", syslogEvent.timestamp);
            record.put("Message", syslogEvent.Message);
            record.put("dynamic", syslogEvent.dynamic);*/
            
             record.put("srcip","srcip");
       		 record.put("ad","ad");
       		 record.put("ts","ts");
       		 record.put("url","url");
       		 record.put("ref","ref");
       		 record.put("ua","ua");
       		 record.put("dstip","dstip");
       		 record.put("cookie","cookie");
       		 record.put("src_port","src_port");

            writeRecord(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
        	MyEventSerializer writer = new MyEventSerializer();
            writer.configure(context);
            return writer;
        }
    }
}