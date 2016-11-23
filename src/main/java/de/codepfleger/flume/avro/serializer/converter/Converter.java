package de.codepfleger.flume.avro.serializer.converter;

import org.apache.avro.Schema;

public interface Converter<T> {
    T convertToTypedObject(Object untyped) throws Exception;
    Schema.Type convertToType(Object typed);
}
