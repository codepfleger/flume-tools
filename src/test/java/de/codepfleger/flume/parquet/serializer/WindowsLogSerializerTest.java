package de.codepfleger.flume.parquet.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static de.codepfleger.flume.avro.serializer.serializer.JsonTestData.*;

@Ignore
public class WindowsLogSerializerTest {
    private WindowsLogSerializer sut;

    @Before
    public void startUp() {
        sut = new WindowsLogSerializer();
        Context context = new Context();
        context.put(WindowsLogSerializer.FILE_PATH_KEY, "file:///C://dev//projects//flume-avro-serializer//tmp//data.%t.%n.parquet");
        context.put(WindowsLogSerializer.FILE_SIZE_KEY, "3000");
        sut.configure(context);
    }

    @After
    public void tearDown() throws IOException {
        sut.beforeClose();
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
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        Event event = new SimpleEvent();
        event.setBody(testDaten);
        sut.write(event);
    }
}