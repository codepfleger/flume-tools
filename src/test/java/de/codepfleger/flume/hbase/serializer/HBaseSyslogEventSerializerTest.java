package de.codepfleger.flume.hbase.serializer;

import de.codepfleger.flume.parquet.serializer.SyslogTestData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Frank Pfleger on 22.03.2017.
 */
public class HBaseSyslogEventSerializerTest {
    @Test
    public void getActions() throws Exception {
        HBaseSyslogEventSerializer serializer = new HBaseSyslogEventSerializer();
        Context context = new Context();
        serializer.configure(context);

        Event event = new SimpleEvent();
        event.setBody(SyslogTestData.TEST_INPUT_1.getBytes());

        serializer.initialize(event, "d".getBytes());

        List<Row> actions = serializer.getActions();

        assertNotNull(actions);
        assertEquals(1, actions.size());
    }
}