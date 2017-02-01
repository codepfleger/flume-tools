package de.codepfleger.flume.parquet.serializer;

import de.codepfleger.flume.avro.serializer.event.SyslogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static de.codepfleger.flume.parquet.serializer.SyslogTestData.TEST_INPUT_1;

public class SyslogSerializerTest {
    private SyslogSerializer sut;

    @Before
    public void startUp() throws IOException {
        sut = new SyslogSerializer();
        Context context = new Context();
        sut.configure(context);
        Schema schema = new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(SyslogEvent.class));
        Path fileToWrite = new Path("tmp//data" + System.currentTimeMillis() + ".parquet");
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
        sut.initialize(writer);
    }

    @After
    public void tearDown() throws IOException {
        sut.close();
    }

    @Test
    public void testEventCreation() throws Exception {
        testEventCreation(TEST_INPUT_1.getBytes());
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        Event event = new SimpleEvent();
        event.setBody(testDaten);
        sut.write(event);
    }
}