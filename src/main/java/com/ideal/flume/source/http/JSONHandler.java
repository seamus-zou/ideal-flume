package com.ideal.flume.source.http;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.JsonSyntaxException;

public class JSONHandler implements HTTPSourceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JSONHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<String, String[]> TABLE_COLUMNS_MAP = new HashMap<String, String[]>();

    private volatile String tableConf;

    @Override
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        // UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
        // be assumed.
        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8") || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {
            LOG.error("Unsupported character set in request {}. " + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException(
                    "JSON handler supports UTF-8, " + "UTF-16 and UTF-32 only.");
        }

        Object jsonObj = null;
        try {
            jsonObj = mapper.readValue(reader, Object.class);
        } catch (JsonSyntaxException ex) {
            throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
        }

        return getSimpleEvents(jsonObj);
    }

    @Override
    public void configure(Context context) {
        tableConf = context.getString("tableConf");
        Preconditions.checkState(StringUtils.isNotBlank(tableConf),
                "qrqm HTTPSource requires a table conf file.");

        try {
            initTableConf();
        } catch (Exception ex) {
            LOG.error("Error configuring JSONHandler!", ex);
            Throwables.propagate(ex);
        }
    }

    private void initTableConf() throws Exception {
        File confFile = new File(tableConf);
        if (!confFile.exists()) {
            throw new IllegalArgumentException("conf file does not exist.");
        }

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(confFile)));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isBlank(line) || line.charAt(0) == '#') {
                    continue;
                }

                String[] arr = line.split(":");
                if (arr.length != 2) {
                    throw new IllegalArgumentException("error code: " + line);
                }

                TABLE_COLUMNS_MAP.put(arr[0], arr[1].split(","));
            }
        } finally {
            reader.close();
        }
    }

    @SuppressWarnings({"rawtypes"})
    private List<Event> getSimpleEvents(Object jsonObj) {
        List<Event> ret = Lists.newArrayList();
        if (jsonObj instanceof Map) {
            Event e = parseEvent((Map) jsonObj);
            if (null != e) {
                ret.add(e);
            }
        } else if (jsonObj instanceof List) {
            for (Object data : (List) jsonObj) {
                Event e = parseEvent((Map) data);
                if (null != e) {
                    ret.add(e);
                }
            }
        }
        return ret;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Event parseEvent(Map data) {
        String channel = (String) data.get("channel");
        if (StringUtils.isBlank(channel)) {
            throw new HTTPBadRequestException("unkonwn channel, required key 'channel'");
        }

        String table = (String) data.get("table");
        if (StringUtils.isBlank(table)) {
            throw new HTTPBadRequestException("unkonwn table, required key 'table'");
        }

        String[] columns = TABLE_COLUMNS_MAP.get(table);
        if (null == columns) {
            throw new HTTPBadRequestException("unkonwn table name, check the table conf");
        }

        String dataStr = (String) data.get("data");
        if (StringUtils.isBlank(dataStr)) {
            throw new HTTPBadRequestException("unkonwn data, required key 'data'");
        }

        Map<String, Object> valueMap;
        try {
            valueMap = mapper.readValue(dataStr, HashMap.class);
        } catch (IOException e) {
            throw new HTTPBadRequestException("Request data has invalid JSON Syntax.");
        }

        List<String> vs = new ArrayList<String>(columns.length);
        for (String column : columns) {
            Object v = valueMap.get(column);
            if (null == v) {
                vs.add(StringUtils.EMPTY);
            } else {
                vs.add(v.toString());
            }
        }

        Event event = new SimpleEvent();
        event.getHeaders().put("%{table}", table);
        event.getHeaders().put("%{channel}", channel);
        event.setBody(StringUtils.join(vs, '|').getBytes(Charsets.UTF_8));
        return event;
    }

}
