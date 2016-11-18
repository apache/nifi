package org.apache.nifi.processors.azure.storage.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableEntity;

/**
 * A Table Entity translating JSON documents to Azure Storage compatible entities
 * 
 * @author Simon Elliston Ball <sball@hortonworks.com>
 */
public class AzureTableEntity implements TableEntity {
    private String etag;
    private String partitionKey;
    private String rowKey;
    private Date timestamp;
    private HashMap<String, EntityProperty> map;

    @Override
    public void readEntity(HashMap<String, EntityProperty> properties, OperationContext opContext) throws StorageException {
        setMap(properties);
    }

    @Override
    public HashMap<String, EntityProperty> writeEntity(OperationContext opContext) throws StorageException {
        return getMap();
    }

    public final String getEtag() {
        return etag;
    }

    public final String getPartitionKey() {
        return partitionKey;
    }

    public final String getRowKey() {
        return rowKey;
    }

    public final Date getTimestamp() {
        return timestamp;
    }

    public HashMap<String, EntityProperty> getMap() {
        return map;
    }

    public final void setEtag(String etag) {
        this.etag = etag;
    }

    public final void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public final void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public final void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public void setMap(HashMap<String, EntityProperty> map) {
        this.map = map;
    }

    public static AzureTableEntity readFromJson(InputStream in) throws JsonProcessingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        rootNodeRef.set(mapper.readTree(in));
        AzureTableEntity entity = new AzureTableEntity();

        Iterator<Entry<String, JsonNode>> fields = rootNodeRef.get().getFields();
        HashMap<String, EntityProperty> map = new HashMap<String, EntityProperty>();

        while (fields.hasNext()) {
            Entry<String, JsonNode> next = fields.next();
            EntityProperty ep = new EntityProperty(next.getValue().asText());
            map.put(next.getKey(), ep);
        }
        entity.setMap(map);
        return entity;
    }
}