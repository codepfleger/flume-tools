package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.SyslogSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static de.codepfleger.flume.parquet.serializer.SyslogTestData.TEST_INPUT_1;

public class HDFSParquetSinkSyslogTest {
    private HDFSParquetSink sut;
    private MemoryChannel memoryChannel;

    @Before
    public void startUp() throws IOException {
        String filePath = "tmp/";
        String fileName = "data.%[n].parquet";
        String serializerType = SyslogSerializer.Builder.class.getName();

        Context context = new Context();
        memoryChannel = new MemoryChannel();
        memoryChannel.configure(new Context());
        memoryChannel.start();

        context.put(HDFSParquetSink.FILE_PATH_KEY, filePath);
        context.put(HDFSParquetSink.FILE_NAME_KEY, fileName);
        context.put(HDFSParquetSink.FILE_SIZE_KEY, "3000");
        context.put(HDFSParquetSink.EVENTS_PER_TRANSACTION_KEY, "1");
        context.put("serializer", serializerType);

        sut = new HDFSParquetSink();
        sut.configure(context);
        sut.setChannel(memoryChannel);
        sut.start();
    }

    @After
    public void tearDown() throws IOException {
        memoryChannel.stop();
        sut.stop();
    }

    @Test
    public void testEventCreation() throws Exception {
        testEventCreation(TEST_INPUT_1.getBytes());

        sut.process();
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        Event event = new SimpleEvent();
        event.setBody(testDaten);
        memoryChannel.getTransaction().begin();
        memoryChannel.put(event);
        memoryChannel.getTransaction().commit();
        memoryChannel.getTransaction().close();
    }
}