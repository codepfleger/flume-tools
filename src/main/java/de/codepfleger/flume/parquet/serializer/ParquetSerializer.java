package de.codepfleger.flume.parquet.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

public interface ParquetSerializer extends EventSerializer, Configurable {
    void initialize(String filePath, Schema fileToWrite) throws IOException;
    ParquetWriter<GenericData.Record> getWriter();
    void close() throws IOException;
}
