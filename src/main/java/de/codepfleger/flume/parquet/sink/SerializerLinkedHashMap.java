package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.ParquetSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SerializerLinkedHashMap extends LinkedHashMap<String, ParquetSerializer> {
    private static final Logger LOG = LoggerFactory.getLogger(SerializerLinkedHashMap.class);

    private final int maxOpenFiles;

    public SerializerLinkedHashMap(int maxOpenFiles) {
        super(maxOpenFiles, 0.75f, true); // stock initial capacity/load, access ordering
        this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, ParquetSerializer> eldest) {
        if (size() > maxOpenFiles) {
            try {
                eldest.getValue().close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
            return true;
        } else {
            return false;
        }
    }
}
