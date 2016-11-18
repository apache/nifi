package org.apache.nifi.processors.azure.storage;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;

import org.apache.nifi.processors.azure.storage.utils.AzureTableEntity;
import org.codehaus.jackson.JsonProcessingException;
import org.junit.Test;

import com.microsoft.azure.storage.table.EntityProperty;

public class TestAzureTableEntity {

    @Test
    public void test() throws JsonProcessingException, IOException {
        AzureTableEntity entity = AzureTableEntity.readFromJson(getClass().getResourceAsStream("/table.json"));
        
        assertNotNull(entity.getMap());
        HashMap<String, EntityProperty> map = entity.getMap();
        assertEquals(4, map.size());
        assertEquals("String Value", map.get("testString").getValueAsString());
    }

}
