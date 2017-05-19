package de.codepfleger.flume.avro.serializer.serializer;

import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.avro.file.DataFileStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertNotNull;

public class DynamicJsonSerializerTest {
    @Test
    public void testEventCreation() throws Exception {
        testEventCreation(JsonTestData.TEST_INPUT_1.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_2.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_3.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_4.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_5.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_6.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_7.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_8.getBytes());
        testEventCreation(JsonTestData.TEST_INPUT_9.getBytes());
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        OutputStream out = new ByteArrayOutputStream();
        DynamicJsonSerializer sut = new DynamicJsonSerializer(out);
        sut.configure(new Context());

        Event event = new SimpleEvent();
        event.setBody(testDaten);

        sut.write(event);
        sut.flush();

        String serializedAvroMessage = out.toString();
        System.out.println(serializedAvroMessage);
        assertNotNull(serializedAvroMessage);
    }

    @Test
    public void testEventRead() throws Exception {
        testEventRead(JsonTestData.TEST_INPUT_1.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_2.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_3.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_4.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_5.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_6.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_7.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_8.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_9.getBytes());
        testEventRead(JsonTestData.TEST_INPUT_10.getBytes());
    }

    public void testEventRead(byte[] testDaten) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DynamicJsonSerializer sut = new DynamicJsonSerializer(out);
        sut.configure(new Context());

        Event event = new SimpleEvent();
        event.setBody(testDaten);

        sut.write(event);
        sut.flush();

        byte[] bytes = out.toByteArray();
        System.out.println("Original:");
        System.out.println(bytes.length);
        System.out.println(new String(bytes));
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Json.Reader reader = new Json.Reader();
        DataFileStream dfs = new DataFileStream(byteArrayInputStream, reader);

        Schema schema = dfs.getSchema();
        assertNotNull(schema);
    }
}