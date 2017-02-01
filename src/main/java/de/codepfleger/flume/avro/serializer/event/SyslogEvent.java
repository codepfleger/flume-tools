package de.codepfleger.flume.avro.serializer.event;

import java.util.HashMap;
import java.util.Map;

public class SyslogEvent {
    public Integer Severity;
    public Integer Facility;
    public String host;
    public String timestamp;
    public String Message;
    public Map<String, Object> dynamic = new HashMap();
}
