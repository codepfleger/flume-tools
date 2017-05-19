package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.WindowsLogSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static de.codepfleger.flume.parquet.serializer.JsonTestData.*;

public class HDFSParquetSinkWindowsTest {
    private HDFSParquetSink sut;
    private MemoryChannel memoryChannel;

    @Before
    public void startUp() throws IOException {
        String filePath = "tmp/";
        String fileName = "data.%[n].parquet";
        String serializerType = WindowsLogSerializer.Builder.class.getName();

        memoryChannel = new MemoryChannel();
        Context memoryContext = new Context();
        memoryContext.put("capacity", "10000");
        memoryChannel.configure(memoryContext);
        memoryChannel.start();

        Context sinkContext = new Context();
        sinkContext.put(HDFSParquetSink.FILE_PATH_KEY, filePath);
        sinkContext.put(HDFSParquetSink.FILE_NAME_KEY, fileName);
        sinkContext.put(HDFSParquetSink.FILE_SIZE_KEY, "3000000");
        sinkContext.put(HDFSParquetSink.FILE_PAGE_SIZE_KEY, "40960");
        sinkContext.put(HDFSParquetSink.FILE_BLOCK_SIZE_KEY, "40960");
        sinkContext.put(HDFSParquetSink.FILE_COMPRESSION_KEY, CompressionCodecName.UNCOMPRESSED.name());
        sinkContext.put(HDFSParquetSink.EVENTS_PER_TRANSACTION_KEY, "1");
        sinkContext.put("serializer", serializerType);

        sut = new HDFSParquetSink();
        sut.configure(sinkContext);
        sut.setChannel(memoryChannel);
        sut.start();
    }

    @After
    public void tearDown() throws IOException {
        memoryChannel.stop();
        sut.stop();
    }

    @Test
    public void testSimpleEventCreation() throws Exception {
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

    @Test
    @Ignore
    public void testPagedEventCreation() throws Exception {
        int numberOfEvents = 1000;
        for(int i = 0; i<numberOfEvents; i++) {
            testEventCreation(TEST_INPUT_1.getBytes());
        }
        for(int i = 0; i<numberOfEvents; i++) {
            System.out.println("Event: " + i);
            sut.process();
            TimeUnit.MILLISECONDS.sleep(10);
        }
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