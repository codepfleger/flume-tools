package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import static de.codepfleger.flume.avro.serializer.serializer.JsonTestData.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WindowsLogSerializerTest {
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
        OutputStream out = new ByteArrayOutputStream();
        WindowsLogSerializer sut = new WindowsLogSerializer(out);
        sut.configure(new Context());

        Event event = new SimpleEvent();
        event.setBody(testDaten);
        WindowsLogEvent windowsLogEvent = sut.convert(event);

        System.out.println(windowsLogEvent + " " + windowsLogEvent.dynamic.size());
        assertNotNull(windowsLogEvent);
        assertTrue(windowsLogEvent.dynamic.size() > 0);
    }
}