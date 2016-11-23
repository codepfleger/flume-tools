package de.codepfleger.flume.avro.serializer.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.source.SyslogParser;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

public class DynamicSyslogSerializer extends AbstractDynamicAvroSerializer {
    private final SyslogParser syslogParser;

    public DynamicSyslogSerializer(OutputStream out) {
        super(out);
        syslogParser = new SyslogParser();
    }

    protected Map getOrderedData(Event event) throws IOException {
        String syslogMessage = new String(event.getBody());
        Event syslogEvent = syslogParser.parseMessage(syslogMessage, Charset.defaultCharset(), null);
        Map<String, Object> data = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : syslogEvent.getHeaders().entrySet()) {
            data.put(entry.getKey(), schemaCreator.getTypedObject(entry.getValue()));
        }
        data.put("message", new String(syslogEvent.getBody()));
        return data;
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            DynamicSyslogSerializer serializer = new DynamicSyslogSerializer(out);
            serializer.configure(context);
            return serializer;
        }
    }
}