package de.codepfleger.flume.avro.serializer.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertNotNull;

public class DynamicSyslogSerializerTest {
    @Test
    public void testSchema() throws Exception {
        OutputStream out = new ByteArrayOutputStream();
        DynamicSyslogSerializer sut = new DynamicSyslogSerializer(out);
        sut.configure(new Context());

        Event event = new SimpleEvent();
        event.setBody(SyslogTestData.TEST_INPUT_1.getBytes());

        sut.write(event);
        sut.flush();

        String serializedAvroMessage = out.toString();
        System.out.println(serializedAvroMessage);
        assertNotNull(serializedAvroMessage);
    }
}
