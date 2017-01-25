package de.codepfleger.flume.parquet.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSParquetSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSParquetSink.class);

    private String serializerType;
    private Context serializerContext;
    private EventSerializer serializer;

    @Override
    public synchronized void start() {
        serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, null);
        super.start();
    }

    @Override
    public synchronized void stop() {
        if(serializer != null) {
            try {
                serializer.beforeClose();
            } catch (IOException e) {
                LOG.error("", e);
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
            Event event = ch.take();
            serializer.write(event);
            txn.commit();
            return Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            return Status.BACKOFF;
        }
    }

    @Override
    public void configure(Context context) {
        serializerType = context.getString("serializer", "TEXT");
        serializerContext = new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
        LOG.info("Serializer = " + serializerType);
    }
}