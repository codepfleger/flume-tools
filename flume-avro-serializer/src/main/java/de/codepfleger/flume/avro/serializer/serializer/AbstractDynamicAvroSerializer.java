package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.utils.DynamicAvroSchemaCreator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.*;

public abstract class AbstractDynamicAvroSerializer implements EventSerializer, Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAvroEventSerializer.class);

    private final OutputStream out;
    protected final DynamicAvroSchemaCreator schemaCreator;
    private Integer syncIntervalBytes;
    private String compressionCodec;

    public AbstractDynamicAvroSerializer(OutputStream out) {
        this.out = out;
        schemaCreator = new DynamicAvroSchemaCreator();
    }

    protected abstract Map<String,Object> getOrderedData(Event event) throws IOException;

    @Override
    public void configure(Context context) {
        syncIntervalBytes = context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
        compressionCodec = context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
    }

    @Override
    public void afterCreate() throws IOException {
        // no-op
    }

    @Override
    public void afterReopen() throws IOException {
        throw new UnsupportedOperationException("Avro API doesn't support append");
    }

    @Override
    public void write(Event event) throws IOException {
        try {
            Map<String, Object> orderedData = getOrderedData(event);
            List<Object> orderedList = new ArrayList<>(orderedData.values());
            Schema schema = schemaCreator.createSchema(orderedData);
            GenericRecord record = createGenericRecord(orderedData, orderedList, schema);

            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
            dataFileWriter.setSyncInterval(syncIntervalBytes);

            try {
                CodecFactory codecFactory = CodecFactory.fromString(compressionCodec);
                dataFileWriter.setCodec(codecFactory);
            } catch (AvroRuntimeException e) {
                LOGGER.warn("Unable to instantiate avro codec with name (" + compressionCodec + "). Compression disabled. Exception follows.", e);
            }

            dataFileWriter.create(schema, out);
            dataFileWriter.append(record);
            dataFileWriter.flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private GenericRecord createGenericRecord(final Map<String, Object> orderedData, final List<Object> orderedList, final Schema schema) {
        return new GenericRecord() {
            @Override
            public Schema getSchema() {
                return schema;
            }

            @Override
            public void put(int i, Object v) {
            }

            @Override
            public Object get(int index) {
                return orderedList.get(index);
            }

            @Override
            public void put(String key, Object v) {
            }

            @Override
            public Object get(String key) {
                return orderedData.get(key);
            }
        };
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }


    @Override
    public void beforeClose() throws IOException {
        // no-op
    }

    @Override
    public boolean supportsReopen() {
        return false;
    }
}