package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.event.SyslogEvent;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import static de.codepfleger.flume.avro.serializer.serializer.SyslogTestData.TEST_INPUT_1;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SyslogSerializerTest {
    @Test
    public void testEventCreation() throws Exception {
        testEventCreation(TEST_INPUT_1.getBytes());
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        OutputStream out = new ByteArrayOutputStream();
        SyslogSerializer sut = new SyslogSerializer(out);
        sut.configure(new Context());

        Event event = new SimpleEvent();
        event.setBody(testDaten);
        SyslogEvent syslogEvent = sut.convert(event);

        System.out.println(syslogEvent + " " + syslogEvent.dynamic.size());
        assertNotNull(syslogEvent);
        assertTrue(syslogEvent.dynamic.size() > 0);
    }
}