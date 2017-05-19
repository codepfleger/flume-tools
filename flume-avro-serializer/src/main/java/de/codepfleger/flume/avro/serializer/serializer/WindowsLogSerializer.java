package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowsLogSerializer extends AbstractReflectionAvroEventSerializer<WindowsLogEvent> {
    private final ObjectMapper mapper;
    private final OutputStream out;

    public WindowsLogSerializer(OutputStream out) {
        this.mapper = new ObjectMapper();
        this.out = out;
    }

    @Override
    protected Schema getSchema() {
        return new Schema.Parser().parse(createSchema(WindowsLogEvent.class));
    }

    @Override
    protected OutputStream getOutputStream() {
        return out;
    }

    @Override
    protected WindowsLogEvent convert(Event event) {
        try {
            String message = new String(event.getBody());
            Map<String, Object> dataMap = new LinkedHashMap<>(mapper.readValue(message, Map.class));

            WindowsLogEvent windowsLogEvent = new WindowsLogEvent();
            setFieldsAndRemove(windowsLogEvent, dataMap);
            windowsLogEvent.dynamic.putAll(dataMap);
            return windowsLogEvent;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            WindowsLogSerializer writer = new WindowsLogSerializer(out);
            writer.configure(context);
            return writer;
        }
    }
}