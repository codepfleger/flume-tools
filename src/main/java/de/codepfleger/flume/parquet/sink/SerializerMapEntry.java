package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.ParquetSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

public class SerializerMapEntry {
    private String workingPath;
    private String targetPath;
    private ParquetSerializer serializer;
    private long startTime;

    public SerializerMapEntry(String workingPath, String targetPath, ParquetSerializer serializer) {
        this.workingPath = workingPath;
        this.targetPath = targetPath;
        this.serializer = serializer;
        this.startTime = new Date().getTime();
    }

    public ParquetSerializer getSerializer() {
        return serializer;
    }

    public long getStartTime() {
        return startTime;
    }

    public void close() throws IOException {
        serializer.close();
        FileSystem fileSystem = FileSystem.get(new Configuration());
        final Path srcPath = new Path(workingPath);
        final Path dstPath = new Path(targetPath);
        fileSystem.rename(srcPath, dstPath);
    }
}
