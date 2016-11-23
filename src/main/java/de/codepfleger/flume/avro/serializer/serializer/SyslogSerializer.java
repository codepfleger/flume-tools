package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.event.SyslogEvent;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.source.SyslogParser;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class SyslogSerializer extends AbstractReflectionAvroEventSerializer<SyslogEvent> {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final SyslogParser syslogParser;
    private final OutputStream out;

    public SyslogSerializer(OutputStream out) {
        this.out = out;
        syslogParser = new SyslogParser();
    }

    @Override
    protected Schema getSchema() {
        return new Schema.Parser().parse(createSchema(SyslogEvent.class));
    }

    @Override
    protected OutputStream getOutputStream() {
        return out;
    }

    @Override
    protected SyslogEvent convert(Event event) {
        try {
            String syslogMessage = new String(event.getBody());
            Event syslogEvent = syslogParser.parseMessage(syslogMessage, Charset.defaultCharset(), null);
            Map<String, Object> dataMap = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : syslogEvent.getHeaders().entrySet()) {
                if("timestamp".equals(entry.getKey())) {
                    dataMap.put(entry.getKey(), DATE_FORMAT.format(new Date(Long.parseLong(entry.getValue()))));
                } else {
                    dataMap.put(entry.getKey(), entry.getValue());
                }
            }
            dataMap.put("message", new String(syslogEvent.getBody()));

            SyslogEvent unixEvent = new SyslogEvent();
            setFieldsAndRemove(unixEvent, dataMap);
            unixEvent.dynamic.putAll(dataMap);
            return unixEvent;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            SyslogSerializer writer = new SyslogSerializer(out);
            writer.configure(context);
            return writer;
        }
    }
}