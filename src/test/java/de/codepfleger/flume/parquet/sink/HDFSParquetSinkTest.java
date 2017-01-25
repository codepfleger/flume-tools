package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.WindowsLogSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static de.codepfleger.flume.parquet.serializer.JsonTestData.*;

@Ignore
public class HDFSParquetSinkTest {
    private HDFSParquetSink sut;
    private MemoryChannel memoryChannel;

    @Before
    public void startUp() throws IOException {
        String filePath = "file:///C://dev//projects//flume-parquet-sink//tmp//data" + System.currentTimeMillis() + ".parquet";
        String serializerType = WindowsLogSerializer.Builder.class.getName();

        Context context = new Context();
        memoryChannel = new MemoryChannel();
        memoryChannel.configure(new Context());
        memoryChannel.start();

        context.put(HDFSParquetSink.FILE_PATH_KEY, filePath);
        context.put(HDFSParquetSink.FILE_SIZE_KEY, "3000");
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
        testEventCreation(TEST_INPUT_2.getBytes());
        testEventCreation(TEST_INPUT_3.getBytes());
        testEventCreation(TEST_INPUT_4.getBytes());
        testEventCreation(TEST_INPUT_5.getBytes());
        testEventCreation(TEST_INPUT_6.getBytes());
        testEventCreation(TEST_INPUT_7.getBytes());
        testEventCreation(TEST_INPUT_8.getBytes());
        testEventCreation(TEST_INPUT_9.getBytes());

        sut.process();
        sut.process();
        sut.process();
        sut.process();
        sut.process();
        sut.process();
        sut.process();
        sut.process();
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