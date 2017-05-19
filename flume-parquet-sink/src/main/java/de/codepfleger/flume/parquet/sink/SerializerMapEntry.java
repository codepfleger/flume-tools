package de.codepfleger.flume.parquet.sink;

import de.codepfleger.flume.parquet.serializer.ParquetSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

public class SerializerMapEntry {
    private Path workingPath;
    private Configuration configuration;
    private String targetPath;
    private ParquetSerializer serializer;
    private long startTime;

    public SerializerMapEntry(Path workingPath, Configuration configuration, String targetPath, ParquetSerializer serializer) {
        this.workingPath = workingPath;
        this.configuration = configuration;
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
        FileSystem fileSystem = workingPath.getFileSystem(configuration);
        Path dstPath = new Path(targetPath);
        fileSystem.rename(workingPath, dstPath);
    }
}
