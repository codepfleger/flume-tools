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
            e.addColumn(cf, "EventTime".getBytes(charset), windowsLogEvent.EventTime.getBytes(charset));
            e.addColumn(cf, "Hostname".getBytes(charset), windowsLogEvent.Hostname.getBytes(charset));
            e.addColumn(cf, "EventType".getBytes(charset), windowsLogEvent.EventType.getBytes(charset));
            e.addColumn(cf, "Severity".getBytes(charset), windowsLogEvent.Severity.getBytes(charset));
            e.addColumn(cf, "SourceModuleName".getBytes(charset), windowsLogEvent.SourceModuleName.getBytes(charset));
            e.addColumn(cf, "UserID".getBytes(charset), windowsLogEvent.UserID.getBytes(charset));
            e.addColumn(cf, "ProcessID".getBytes(charset), ("" + windowsLogEvent.ProcessID).getBytes(charset));
            e.addColumn(cf, "Domain".getBytes(charset), windowsLogEvent.Domain.getBytes(charset));
            e.addColumn(cf, "EventReceivedTime".getBytes(charset), windowsLogEvent.EventReceivedTime.getBytes(charset));
            e.addColumn(cf, "Path".getBytes(charset), windowsLogEvent.Path.getBytes(charset));
            e.addColumn(cf, "Message".getBytes(charset), windowsLogEvent.Message.getBytes(charset));
//            e.addColumn(cf, "dynamic".getBytes(charset), ("" + windowsLogEvent.dynamic).getBytes(charset));
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
