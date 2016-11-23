package de.codepfleger.flume.avro.serializer.utils;

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by frpflege on 09.11.2016.
 */
public class DynamicAvroSchemaCreatorTest {
    @Test
    public void createSchema() throws Exception {
        String testInput = "{\"EventTime\":\"2016-08-27 00:00:00\",\"Hostname\":\"WVIGRZ0006.t-systems.ch\",\"Keywords\":-9223372036854775808,\"EventType\":\"WARNING\",\"SeverityValue\"" +
                ":3,\"Severity\":\"WARNING\",\"EventID\":1002,\"SourceName\":\"Microsoft-Windows-KnownFolders\",\"ProviderGuid\":\"{8939299F-2315-4C5C-9B91-ABB86AA0627D}\"," +
                "\"Version\":0,\"Task\":0,\"OpcodeValue\":0,\"RecordNumber\":388239,\"ProcessID\":7324,\"ThreadID\":9104,\"Channel\":\"Microsoft-Windows-Known Folders API Se" +
                "rvice\",\"Domain\":\"NT AUTHORITY\",\"AccountName\":\"SYSTEM\",\"UserID\":\"SYSTEM\",\"AccountType\":\"User\",\"Message\":\"Error 0x80070002 occurred while verif" +
                "ying known folder {625B53C3-AB48-4EC1-BA1F-A1EF4146FC19} with path 'C:\\\\Windows\\\\system32\\\\config\\\\systemprofile\\\\AppData\\\\Roaming\\\\Microsoft" +
                "\\\\Windows\\\\Start Menu'.\",\"Opcode\":\"Info\",\"hrError\":\"0x80070002\",\"FolderId\":\"{625B53C3-AB48-4EC1-BA1F-A1EF4146FC19}\",\"Path\":\"C:\\\\Windows\\\\syst" +
                "em32\\\\config\\\\systemprofile\\\\AppData\\\\Roaming\\\\Microsoft\\\\Windows\\\\Start Menu\",\"EventReceivedTime\":\"2016-08-27 00:00:01\",\"SourceModuleName\":\"" +
                "in\",\"SourceModuleType\":\"im_msvistalog\"}";

        String testOutput = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"EventTime\",\"type\":[\"null\",\"string\"]},{\"name\":\"Hostname\",\"type\":[\"null\",\"string\"]},{\"name\":\"Keywords\",\"type\":[\"null\",\"long\"]},{\"name\":\"EventType\",\"type\":[\"null\",\"string\"]},{\"name\":\"SeverityValue\",\"type\":[\"null\",\"int\"]},{\"name\":\"Severity\",\"type\":[\"null\",\"string\"]},{\"name\":\"EventID\",\"type\":[\"null\",\"int\"]},{\"name\":\"SourceName\",\"type\":[\"null\",\"string\"]},{\"name\":\"ProviderGuid\",\"type\":[\"null\",\"string\"]},{\"name\":\"Version\",\"type\":[\"null\",\"int\"]},{\"name\":\"Task\",\"type\":[\"null\",\"int\"]},{\"name\":\"OpcodeValue\",\"type\":[\"null\",\"int\"]},{\"name\":\"RecordNumber\",\"type\":[\"null\",\"int\"]},{\"name\":\"ProcessID\",\"type\":[\"null\",\"int\"]},{\"name\":\"ThreadID\",\"type\":[\"null\",\"int\"]},{\"name\":\"Channel\",\"type\":[\"null\",\"string\"]},{\"name\":\"Domain\",\"type\":[\"null\",\"string\"]},{\"name\":\"AccountName\",\"type\":[\"null\",\"string\"]},{\"name\":\"UserID\",\"type\":[\"null\",\"string\"]},{\"name\":\"AccountType\",\"type\":[\"null\",\"string\"]},{\"name\":\"Message\",\"type\":[\"null\",\"string\"]},{\"name\":\"Opcode\",\"type\":[\"null\",\"string\"]},{\"name\":\"hrError\",\"type\":[\"null\",\"string\"]},{\"name\":\"FolderId\",\"type\":[\"null\",\"string\"]},{\"name\":\"Path\",\"type\":[\"null\",\"string\"]},{\"name\":\"EventReceivedTime\",\"type\":[\"null\",\"string\"]},{\"name\":\"SourceModuleName\",\"type\":[\"null\",\"string\"]},{\"name\":\"SourceModuleType\",\"type\":[\"null\",\"string\"]}]}";

        DynamicAvroSchemaCreator sut = new DynamicAvroSchemaCreator();

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> data = mapper.readValue(testInput, Map.class);
        Schema schema = sut.createSchema(data);

        assertNotNull(schema);
        assertEquals(testOutput, schema.toString());
    }
}