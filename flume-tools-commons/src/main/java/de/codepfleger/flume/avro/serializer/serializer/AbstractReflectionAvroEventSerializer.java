package de.codepfleger.flume.avro.serializer.serializer;

import de.codepfleger.flume.avro.serializer.utils.DynamicAvroSchemaCreator;
import org.apache.flume.serialization.AbstractAvroEventSerializer;

import java.lang.reflect.Field;
import java.util.Map;

public abstract class AbstractReflectionAvroEventSerializer<T> extends AbstractAvroEventSerializer<T> {
    protected static final DynamicAvroSchemaCreator SCHEMA_CREATOR = new DynamicAvroSchemaCreator();

    public static void setFieldsAndRemove(Object eventObject, Map<String, Object> dataMap) throws Exception {
        for (Field field : eventObject.getClass().getDeclaredFields()) {
            Class<?> type = field.getType();
            Object property = dataMap.get(field.getName());
            if (property != null) {
                field.set(eventObject, SCHEMA_CREATOR.convertByType(type, property));
                dataMap.remove(field.getName());
            }
        }
    }

    public static String createSchema(Class<?> clazz) {
        StringBuilder schema = new StringBuilder();
        schema.append("{ \"type\":\"record\", \"name\": \"Event\", \"fields\": [");

        boolean first = true;
        for (Field field : clazz.getDeclaredFields()) {
            if (!first) {
                schema.append(",");
            } else {
                first = false;
            }

            String type = exractType(field);
            if ("map".equals(type)) {
                schema.append("{\"name\": \"" + field.getName() + "\", \"type\": { \"type\": \"map\", \"values\": \"string\" } } ");
            } else {
                schema.append("{\"name\": \"" + field.getName() + "\", \"type\": [\"null\", \"" + type + "\"] }");
            }
        }

        schema.append("] }");

        return schema.toString();
    }

    private static String exractType(Field field) {
        if (field.getType().isAssignableFrom(String.class)) {
            return "string";
        } else if (field.getType().isAssignableFrom(Integer.class)) {
            return "int";
        } else if (field.getType().isAssignableFrom(Long.class)) {
            return "long";
        } else if (field.getType().isAssignableFrom(Boolean.class)) {
            return "boolean";
        } else if (field.getType().isAssignableFrom(Map.class)) {
            return "map";
        } else {
            throw new IllegalArgumentException(field.getType().getSimpleName());
        }
    }
}