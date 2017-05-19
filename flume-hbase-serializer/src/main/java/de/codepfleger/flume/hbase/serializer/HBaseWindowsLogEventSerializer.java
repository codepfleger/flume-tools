package de.codepfleger.flume.hbase.serializer;

import com.google.common.collect.Lists;
import de.codepfleger.flume.avro.serializer.event.WindowsLogEvent;
import de.codepfleger.flume.avro.serializer.serializer.AbstractReflectionAvroEventSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Put;
import org.codehaus.jackson.map.ObjectMapper;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Frank Pfleger on 22.03.2017.
 */
public class HBaseWindowsLogEventSerializer implements HbaseEventSerializer {
    private final ObjectMapper mapper;

    private Charset charset;

    protected byte[] cf;
    private byte[] payload;

    public HBaseWindowsLogEventSerializer() {
        this.mapper = new ObjectMapper();
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

            String message = new String(payload, charset);
            Map<String, Object> dataMap = new LinkedHashMap<>(mapper.readValue(message, Map.class));
            WindowsLogEvent windowsLogEvent = new WindowsLogEvent();
            AbstractReflectionAvroEventSerializer.setFieldsAndRemove(windowsLogEvent, dataMap);
            windowsLogEvent.dynamic.putAll(dataMap);

            Put e = new Put(getRowKey(windowsLogEvent.Hostname));
            e.addColumn(cf, "EventTime".getBytes(charset), getBytes(windowsLogEvent.EventTime));
            e.addColumn(cf, "Hostname".getBytes(charset), getBytes(windowsLogEvent.Hostname));
            e.addColumn(cf, "EventType".getBytes(charset), getBytes(windowsLogEvent.EventType));
            e.addColumn(cf, "Severity".getBytes(charset), getBytes(windowsLogEvent.Severity));
            e.addColumn(cf, "SourceModuleName".getBytes(charset), getBytes(windowsLogEvent.SourceModuleName));
            e.addColumn(cf, "UserID".getBytes(charset), getBytes(windowsLogEvent.UserID));
            e.addColumn(cf, "ProcessID".getBytes(charset), getBytes(("" + windowsLogEvent.ProcessID)));
            e.addColumn(cf, "Domain".getBytes(charset), getBytes(windowsLogEvent.Domain));
            e.addColumn(cf, "EventReceivedTime".getBytes(charset), getBytes(windowsLogEvent.EventReceivedTime));
            e.addColumn(cf, "Path".getBytes(charset), getBytes(windowsLogEvent.Path));
            e.addColumn(cf, "Message".getBytes(charset), getBytes(windowsLogEvent.Message));
//            e.addColumn(cf, "dynamic".getBytes(charset), ("" + windowsLogEvent.dynamic).getBytes(charset));
            actions.add(e);

            return actions;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getBytes(String object) {
        return object == null ? "".getBytes(charset) : object.getBytes(charset);
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
