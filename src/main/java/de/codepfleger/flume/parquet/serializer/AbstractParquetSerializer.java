package de.codepfleger.flume.parquet.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractParquetSerializer implements ParquetSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractParquetSerializer.class);

    private ParquetWriter<GenericData.Record> writer;
    private Schema schema;

    protected AbstractParquetSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void configure(Context context) {
    }

    @Override
    public void afterCreate() throws IOException {
    }

    @Override
    public void afterReopen() throws IOException {
    }

    protected synchronized void writeRecord(GenericData.Record record) throws IOException {
        writer.write(record);
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

    public void initialize(ParquetWriter<GenericData.Record> writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public ParquetWriter<GenericData.Record> getWriter() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        beforeClose();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}