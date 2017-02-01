package de.codepfleger.flume.parquet.serializer;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowsLogSerializer extends AbstractParquetSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(WindowsLogSerializer.class);

    private final ObjectMapper mapper;

    public WindowsLogSerializer() {
        super(new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(WindowsLogEvent.class)));
        this.mapper = new ObjectMapper();
    }

    @Override
    public void write(Event event) throws IOException {
        try {
            String message = new String(event.getBody());
            Map<String, Object> dataMap = new LinkedHashMap<>(mapper.readValue(message, Map.class));
            WindowsLogEvent windowsLogEvent = new WindowsLogEvent();
            AbstractReflectionAvroEventSerializer.setFieldsAndRemove(windowsLogEvent, dataMap);
            windowsLogEvent.dynamic.putAll(dataMap);

            GenericData.Record record = new GenericData.Record(getSchema());
            record.put("EventTime", windowsLogEvent.EventTime);
            record.put("Hostname", windowsLogEvent.Hostname);
            record.put("EventType", windowsLogEvent.EventType);
            record.put("Severity", windowsLogEvent.Severity);
            record.put("SourceModuleName", windowsLogEvent.SourceModuleName);
            record.put("UserID", windowsLogEvent.UserID);
            record.put("ProcessID", windowsLogEvent.ProcessID);
            record.put("Domain", windowsLogEvent.Domain);
            record.put("EventReceivedTime", windowsLogEvent.EventReceivedTime);
            record.put("Path", windowsLogEvent.Path);
            record.put("Message", windowsLogEvent.Message);
            record.put("dynamic", windowsLogEvent.dynamic);

            writeRecord(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            WindowsLogSerializer writer = new WindowsLogSerializer();
            writer.configure(context);
            return writer;
        }
    }
}