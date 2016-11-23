package de.codepfleger.flume.avro.serializer.converter;

import org.apache.avro.Schema;

public class LongConverter implements Converter<Long> {
    @Override
    public Long convertToTypedObject(Object untyped) throws Exception {
        return  Long.parseLong(String.valueOf(untyped));
    }

    @Override
    public Schema.Type convertToType(Object typed) {
        return typed instanceof Long ? Schema.Type.LONG : null;
    }
}
