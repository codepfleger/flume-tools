package de.codepfleger.flume.avro.serializer.converter;

import org.apache.avro.Schema;

public class IntegerConverter implements Converter<Integer> {
    @Override
    public Integer convertToTypedObject(Object untyped) throws Exception {
        return Integer.parseInt(String.valueOf(untyped));
    }

    @Override
    public Schema.Type convertToType(Object typed) {
        return typed instanceof Integer ? Schema.Type.INT : null;
    }
}
