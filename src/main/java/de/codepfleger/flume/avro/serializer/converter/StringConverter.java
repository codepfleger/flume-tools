package de.codepfleger.flume.avro.serializer.converter;

import org.apache.avro.Schema;

public class StringConverter implements Converter<String> {
    @Override
    public String convertToTypedObject(Object untyped) throws Exception {
        return String.valueOf(untyped);
    }

    @Override
    public Schema.Type convertToType(Object typed) {
        return typed instanceof String ? Schema.Type.STRING : null;
    }
}
