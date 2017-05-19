package de.codepfleger.flume.hbase.serializer;

import com.google.common.collect.Lists;
import de.codepfleger.flume.avro.serializer.event.SyslogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.flume.source.SyslogParser;
import org.apache.hadoop.hbase.client.Put;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Frank Pfleger on 22.03.2017.
 */
public class HBaseSyslogEventSerializer implements HbaseEventSerializer {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final SyslogParser syslogParser;

    private Charset charset;

    protected byte[] cf;
    private byte[] payload;

    public HBaseSyslogEventSerializer() {
        syslogParser = new SyslogParser();
    }

    public void configure(Context context) {
        this.charset = Charset.forName(context.getString("charset", "UTF-8"));
    }

    @Override
    public void initialize(Event event, byte[] columnFamily) {
        this.payload = event.getBody();
        this.cf = columnFamily;
    }

    protected byte[] getRowKey(String hostname) {
        String rowKey = String.format("%s-%s", new Object[]{UUID.randomUUID(), hostname});
        return rowKey.getBytes(this.charset);
    }

    @Override
    public List<org.apache.hadoop.hbase.client.Row> getActions() {
        try {
            List<org.apache.hadoop.hbase.client.Row> actions = Lists.newArrayList();

            String syslogMessage = new String(payload);
            Event event = syslogParser.parseMessage(syslogMessage, Charset.defaultCharset(), null);
            Map<String, Object> dataMap = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                if ("timestamp".equals(entry.getKey())) {
                    dataMap.put(entry.getKey(), DATE_FORMAT.format(new Date(Long.parseLong(entry.getValue()))));
                } else {
                    dataMap.put(entry.getKey(), entry.getValue());
                }
            }
            dataMap.put("Message", new String(event.getBody()));

            SyslogEvent syslogEvent = new SyslogEvent();
            AbstractReflectionAvroEventSerializer.setFieldsAndRemove(syslogEvent, dataMap);
            syslogEvent.dynamic.putAll(dataMap);

            Put e = new Put(getRowKey(syslogEvent.host));
            e.addColumn(cf, "Severity".getBytes(charset), ("" + syslogEvent.Severity).getBytes(charset));
            e.addColumn(cf, "Facility".getBytes(charset), ("" + syslogEvent.Facility).getBytes(charset));
            e.addColumn(cf, "host".getBytes(charset), syslogEvent.host.getBytes(charset));
            e.addColumn(cf, "timestamp".getBytes(charset), syslogEvent.timestamp.getBytes(charset));
            e.addColumn(cf, "Message".getBytes(charset), syslogEvent.Message.getBytes(charset));
//            e.addColumn(cf, "dynamic".getBytes(charset), syslogEvent.dynamic);
            actions.add(e);

            return actions;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<org.apache.hadoop.hbase.client.Increment> getIncrements() {
        return Lists.newArrayList();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }
}