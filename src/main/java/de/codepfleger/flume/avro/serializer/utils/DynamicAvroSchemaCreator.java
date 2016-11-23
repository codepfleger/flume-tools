package de.codepfleger.flume.avro.serializer.utils;

import de.codepfleger.flume.avro.serializer.converter.Converter;
import de.codepfleger.flume.avro.serializer.converter.IntegerConverter;
import de.codepfleger.flume.avro.serializer.converter.LongConverter;
import de.codepfleger.flume.avro.serializer.converter.StringConverter;
import org.apache.avro.Schema;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DynamicAvroSchemaCreator {
    private final Map<Class, Converter> convertersMap;

    public DynamicAvroSchemaCreator() {
        convertersMap = new LinkedHashMap();
        convertersMap.put(Integer.class, new IntegerConverter());
        convertersMap.put(Long.class, new LongConverter());
        convertersMap.put(String.class, new StringConverter());
    }

    public Schema createSchema(Map<String, Object> data) throws Exception {
        List<Schema.Field> fields = createFields(data);
        Schema schema = Schema.createRecord("Event", null, null, false);
        Method method = schema.getClass().getDeclaredMethod("setFields", List.class);
        method.setAccessible(true);
        method.invoke(schema, fields);
        return schema;
    }

    private List<Schema.Field> createFields(Map<String, Object> data) throws IOException {
        List<Schema.Field> fields = new ArrayList<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            List<Schema> types = new ArrayList<>();
            types.add(Schema.create(Schema.Type.NULL));
            types.add(Schema.create(deduceType(entry)));
            Schema typeSchema = Schema.createUnion(types);
            Schema.Field field = new Schema.Field(entry.getKey(), typeSchema, null, null, Schema.Field.Order.ASCENDING);
            fields.add(field);
        }

        return fields;
    }

    public Schema.Type deduceType(Map.Entry<String, Object> entry) {
        Object typed = getTypedObject(entry.getValue());

        for (Converter converter : convertersMap.values()) {
            Schema.Type type = converter.convertToType(typed);
            if(type != null) {
                return type;
            }
        }

        throw new IllegalStateException("Unmappable type: " + typed);
    }

    public Object getTypedObject(Object untyped) {
        for (Converter converter : convertersMap.values()) {
            try {
                Object typed = converter.convertToTypedObject(untyped);
                return typed;
            } catch (Exception e) {
            }
        }

        throw new IllegalStateException("Unmappable type: " + untyped);
    }

    public <T> T convertByType(Class<T> type, Object untyped) throws Exception {
        Converter<T> converter = convertersMap.get(type);
        return converter.convertToTypedObject(untyped);
    }
}