package de.codepfleger.flume.parquet.serializer;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowsLogSerializer implements EventSerializer, Configurable {
    public static final String FILE_PATH_KEY = "filePath";

    private final ObjectMapper mapper;
    private ParquetWriter<GenericData.Record> writer;

    public WindowsLogSerializer() {
        this.mapper = new ObjectMapper();
    }

    protected Schema getSchema() {
        return new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(WindowsLogEvent.class));
    }

    @Override
    public void configure(Context context) {
        String filePath = context.getString(FILE_PATH_KEY);
        if(filePath == null) {
            throw new IllegalStateException("filePath missing");
        } else if(filePath.contains("%t")) {
            filePath = filePath.replaceAll("%t", "" + System.currentTimeMillis());
        }

        Path fileToWrite = new Path(filePath);
//        filePath.length()
        try {
            writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                    .withSchema(getSchema())
                    .withCompressionCodec(CompressionCodecName.SNAPPY).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterCreate() throws IOException {

    }

    @Override
    public void afterReopen() throws IOException {

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
            writer.write(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void beforeClose() throws IOException {
        writer.close();
    }

    @Override
    public boolean supportsReopen() {
        return false;
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