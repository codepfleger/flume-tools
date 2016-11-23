package de.codepfleger.flume.avro.serializer.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by frpflege on 09.11.2016.
 */
public class DynamicJsonSerializer extends AbstractDynamicAvroSerializer {
    private final ObjectMapper mapper;

    public DynamicJsonSerializer(OutputStream out) {
        super(out);
        mapper = new ObjectMapper();
    }

    @Override
    protected Map getOrderedData(Event event) throws IOException {
        String message = new String(event.getBody());
        return new LinkedHashMap<>(mapper.readValue(message, Map.class));
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            DynamicJsonSerializer serializer = new DynamicJsonSerializer(out);
            serializer.configure(context);
            return serializer;
        }
    }
}