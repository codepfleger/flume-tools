# flume-avro-serializer

**Public Home**

https://github.com/codepfleger/flume-avro-serializer

This library allows you to store Avro files in HDFS using a specific but dynamic avro-schema.
There is no need to specify the avro-schema itself, as it will be deduced.
There are 4 predefined serializer for Windows Log-Events and Unix-Syslog Events and dynamic events.

1. de.codepfleger.flume.avro.serializer.serializer.WindowsLogSerializer
2. de.codepfleger.flume.avro.serializer.serializer.SyslogSerializer
3. de.codepfleger.flume.avro.serializer.serializer.DynamicJsonSerializer
4. de.codepfleger.flume.avro.serializer.serializer.DynamicSyslogSerializer

These serializers can be used out-of-the box by specifying the builder in your Flume configuration.

1. de.codepfleger.flume.avro.serializer.serializer.WindowsLogSerializer$Builder
2. de.codepfleger.flume.avro.serializer.serializer.SyslogSerializer$Builder
3. de.codepfleger.flume.avro.serializer.serializer.DynamicJsonSerializer$Builder
4. de.codepfleger.flume.avro.serializer.serializer.DynamicSyslogSerializer$Builder

The messages will be written either in a half static and half dynamic format or in a fully dynamic format.

**Half dynamic**

_Windows Event Static_: String EventTime, String Hostname, String EventType, String Severity, String SourceModuleName, String UserID, Integer ProcessID, String Domain, String EventReceivedTime, String Path, String Message
_Windows Event Dynamic (all other fields)_: Map<String, Object> dynamic = new HashMap<>()

_Syslog Event Static_: Integer Severity, Integer Facility, String host, String timestamp
_Syslog Event Dynamic (all other fields)_: Map<String, Object> dynamic = new HashMap<>()

**Fully dynamic**

The dynamic derializers DynamicJsonSerializer and DynamicSyslogSerializer will create a schema based on the data of the message.
The property types will be deduced from the data sent.

**Custom serializer**

There are 2 abstract classes that can be used to create custom serializers with custom events.
In both cases there is no need to specify a schema, as it will be deduced.

1. AbstractReflectionAvroEventSerializer<T> -> The schema will be deduced from the generic type parameter
2. AbstractDynamicAvroSerializer -> the schema will be deduced from the data.