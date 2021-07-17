/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.exception.ProcessException;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Util class for schema manipulation
 */
public class BigQueryUtils {
    /**
     * Exception thrown when a given type can't be transformed into a valid BigQuery type.
     *
     */
    public static class BadTypeNameException extends ProcessException {
        public BadTypeNameException(String message) {
            super(message);
        }
    }


    private final static Type gsonSchemaType = new TypeToken<List<Map>>() { }.getType();

    public static Field mapToField(Map fMap) {
        String typeStr = fMap.get("type").toString().toUpperCase();
        String nameStr = fMap.get("name").toString();
        String modeStr;
        if(fMap.containsKey("mode")) {
            modeStr = fMap.get("mode").toString();
        } else {
            modeStr = Mode.NULLABLE.name();
        }
        LegacySQLTypeName type = null;
        List<Field> subFields = new ArrayList<>();

        switch(typeStr) {
        case "BOOLEAN":
            type = LegacySQLTypeName.BOOLEAN;
            break;
        case "STRING":
            type = LegacySQLTypeName.STRING;
            break;
        case "BYTES":
            type = LegacySQLTypeName.BYTES;
            break;
        case "INTEGER":
            type = LegacySQLTypeName.INTEGER;
            break;
        case "FLOAT":
            type = LegacySQLTypeName.FLOAT;
            break;
        case "RECORD":
            type = LegacySQLTypeName.RECORD;
            List<Map> fields = (List<Map>) fMap.get("fields");
            subFields.addAll(listToFields(fields));
            break;
        case "TIMESTAMP":
        case "DATE":
        case "TIME":
        case "DATETIME":
            type = LegacySQLTypeName.TIMESTAMP;
            break;
        default:
            throw new BadTypeNameException(String.format("You used invalid BigQuery type \"%s\" in declaration of\n%s\n"
                    + "Supported types are \"BOOLEAN, STRING, BYTES, INTEGER, FLOAT, RECORD, TIMESTAMP, DATE, TIME, DATETIME\"",
                    typeStr, fMap));
        }

        return Field.newBuilder(nameStr, type, subFields.toArray(new Field[subFields.size()])).setMode(Field.Mode.valueOf(modeStr)).build();
    }

    public static List<Field> listToFields(List<Map> fieldsDescriptors) {
        List<Field> fields = new ArrayList(fieldsDescriptors.size());
        for (Map m : fieldsDescriptors) {
            fields.add(mapToField(m));
        }

        return fields;
    }

    /**
     * Parse a schema defintion into a schema
     */
    public static Schema schemaFromString(String schemaStr) {
        if (schemaStr == null) {
            return null;
        } else {
            Gson gson = new Gson();
            List<Map> fields = gson.fromJson(schemaStr, gsonSchemaType);
            return Schema.of(BigQueryUtils.listToFields(fields));
        }
    }

}
