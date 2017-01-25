package de.codepfleger.flume.parquet.serializer;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowsLogSerializer implements EventSerializer, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(WindowsLogSerializer.class);

    public static final String FILE_PATH_KEY = "filePath";
    public static final String FILE_SIZE_KEY = "fileSize";

    private final ObjectMapper mapper;

    private String filePath;
    private int fileSize;

    private ParquetWriter<GenericData.Record> writer;
    private Path fileToWrite;
    private AtomicInteger fileNumber = new AtomicInteger(0);

    public WindowsLogSerializer() {
        this.mapper = new ObjectMapper();
    }

    protected Schema getSchema() {
        return new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(WindowsLogEvent.class));
    }

    @Override
    public void configure(Context context) {
        filePath = context.getString(FILE_PATH_KEY);
        if(filePath == null) {
            throw new IllegalStateException("filePath missing");
        }
        fileSize = context.getInteger(FILE_SIZE_KEY, 500000);

        LOG.info("WindowsLogSerializer.filePath = " + filePath);
        LOG.info("WindowsLogSerializer.fileSize = " + fileSize);
    }

    private synchronized void createNewWriter() throws IOException {
        closeWriter(writer);

        String newFilePath = filePath;
        if(newFilePath.contains("%t")) {
            newFilePath = newFilePath.replaceAll("%t", "" + System.currentTimeMillis());
        }
        if(newFilePath.contains("%n")) {
            newFilePath = newFilePath.replaceAll("%n", "" + fileNumber.incrementAndGet());
        }

        newFilePath = BucketPath.escapeString(newFilePath, new HashMap<String, String>(),null, false, 0, 1, true);

        fileToWrite = new Path(newFilePath);
        writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(getSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    private void closeWriter(ParquetWriter<GenericData.Record> writer) throws IOException {
        if(writer != null) {
            writer.close();
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

            writeRecord(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void writeRecord(GenericData.Record record) throws IOException {
        if(writer == null || writer.getDataSize() > fileSize) {
            createNewWriter();
        }

        writer.write(record);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void beforeClose() throws IOException {
        closeWriter(writer);
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