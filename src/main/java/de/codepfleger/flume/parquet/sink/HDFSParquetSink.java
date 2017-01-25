package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import de.codepfleger.flume.parquet.serializer.ParquetSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class HDFSParquetSink extends AbstractSink implements Configurable {
    public static final String FILE_PATH_KEY = "filePath";
    public static final String FILE_SIZE_KEY = "fileSize";

    private static final Logger LOG = LoggerFactory.getLogger(HDFSParquetSink.class);

    private final Object lock = new Object();
    private final Random random = new Random();

    private SerializerLinkedHashMap serializers;

    private String filePath;
    private Integer uncompressedFileSize;
    private String serializerType;
    private Context serializerContext;

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        if(serializers != null) {
            synchronized (lock) {
                for (ParquetSerializer serializer : serializers.values()) {
                    try {
                        serializer.close();
                    } catch (IOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                serializers.clear();
            }
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            //TODO take multiple in one transaction
            Event event = ch.take();
            if (event != null) {
                getSerializer(event).write(event);
            }
            txn.commit();
            return Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            return Status.BACKOFF;
        } finally {
            txn.close();
        }
    }

    private EventSerializer getSerializer(Event event) throws IOException {
        String filePath = getFilePathFromEvent(event);
        synchronized (lock) {
            ParquetSerializer eventSerializer = serializers.get(filePath);
            if(eventSerializer != null) {
                if(eventSerializer.getWriter().getDataSize() > uncompressedFileSize) {
                    eventSerializer.close();
                    serializers.remove(filePath);
                }
            }
            if(eventSerializer == null) {
                eventSerializer = createSerializer(filePath);
                serializers.put(filePath, eventSerializer);
            }
            return eventSerializer;
        }
    }

    private ParquetSerializer createSerializer(String filePath) throws IOException {
        ParquetSerializer eventSerializer;
        eventSerializer = (ParquetSerializer) EventSerializerFactory.getInstance(serializerType, serializerContext, null);
        Path fileToWrite = new Path(getActualFilePath(filePath));
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(getSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
        eventSerializer.initialize(writer, getSchema());
        return eventSerializer;
    }

    protected Schema getSchema() {
        //TODO event class configurable
        return new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(WindowsLogEvent.class));
    }

    private String getActualFilePath(String actualFilePath) {
        if(actualFilePath.contains("%[n]")) {
            actualFilePath = actualFilePath.replace("%[n]", "" + random.nextInt());
        } else {
            actualFilePath += "." + random.nextInt();
        }
        return actualFilePath;
    }

    private String getFilePathFromEvent(Event event) {
        return BucketPath.escapeString(filePath, event.getHeaders(), null, false, 0, 1, true);
    }

    @Override
    public void configure(Context context) {
        filePath = context.getString(FILE_PATH_KEY);
        if(filePath == null) {
            throw new IllegalStateException("filePath missing");
        }
        uncompressedFileSize = context.getInteger(FILE_SIZE_KEY, 500000);
        serializerType = context.getString("serializer", "TEXT");
        serializerContext = new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));

        serializers = new SerializerLinkedHashMap(4);
    }
}