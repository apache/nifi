/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.processors.gcp.bigquery;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.util.Transport;

/**
 *
 * @author Mikhail Sosonkin (Synack Inc, Synack.com)
 */
public class BqUtils {
    private final static Type gsonSchemaType = new TypeToken<List<Map>>(){}.getType();
    
    public static Field mapToField(Map fMap) {
        String typeStr = fMap.get("type").toString();
        String nameStr = fMap.get("name").toString();
        String modeStr = fMap.get("mode").toString();
        Field.Type type = null;
        
        if(typeStr.equals("BOOLEAN")) {
            type = Field.Type.bool();
        } else if(typeStr.equals("STRING")) {
            type = Field.Type.string();
        } else if(typeStr.equals("BYTES")) {
            type = Field.Type.bytes();
        } else if(typeStr.equals("INTEGER")) {
            type = Field.Type.integer();
        } else if(typeStr.equals("FLOAT")) {
            type = Field.Type.floatingPoint();
        } else if(typeStr.equals("TIMESTAMP") || typeStr.equals("DATE") || 
                  typeStr.equals("TIME") || typeStr.equals("DATETIME")) {
            type = Field.Type.timestamp();
        } else if(typeStr.equals("RECORD")) {
            List<Map> m_fields = (List<Map>) fMap.get("fields");
            type = Field.Type.record(listToFields(m_fields));
        }

        return Field.newBuilder(nameStr, type).setMode(Field.Mode.valueOf(modeStr)).build();
    }
    
    public static List<Field> listToFields(List<Map> m_fields) {
        List<Field> fields = new ArrayList(m_fields.size());
        for(Map m : m_fields) {
            fields.add(mapToField(m));
        }
        
        return fields;
    }
    
    public static Schema schemaFromString(String schemaStr) {
        Gson gson = new Gson();
        List<Map> fields = gson.fromJson(schemaStr, gsonSchemaType);

        return Schema.of(BqUtils.listToFields(fields));
    }
    
    public static <T> T fromJsonString(String json, Class<T> clazz) throws IOException {
        JsonFactory JSON_FACTORY = Transport.getJsonFactory();
        
        return JSON_FACTORY.fromString(json, clazz);
    }
        
    public static TableSchema tableSchemaFromString(String schemaStr) throws IOException {
        schemaStr = "{\"fields\":" + schemaStr + "}";
        
        return fromJsonString(schemaStr, TableSchema.class);
    }
}
