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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Util class for schema manipulation
 */
public class BigQueryUtils {

    private final static Type gsonSchemaType = new TypeToken<List<Map>>() { }.getType();

    public static Field mapToField(Map fMap) {
        String typeStr = fMap.get("type").toString();
        String nameStr = fMap.get("name").toString();
        String modeStr = fMap.get("mode").toString();
        LegacySQLTypeName type = null;

        if (typeStr.equals("BOOLEAN")) {
            type = LegacySQLTypeName.BOOLEAN;
        } else if (typeStr.equals("STRING")) {
            type = LegacySQLTypeName.STRING;
        } else if (typeStr.equals("BYTES")) {
            type = LegacySQLTypeName.BYTES;
        } else if (typeStr.equals("INTEGER")) {
            type = LegacySQLTypeName.INTEGER;
        } else if (typeStr.equals("FLOAT")) {
            type = LegacySQLTypeName.FLOAT;
        } else if (typeStr.equals("TIMESTAMP") || typeStr.equals("DATE")
                || typeStr.equals("TIME") || typeStr.equals("DATETIME")) {
            type = LegacySQLTypeName.TIMESTAMP;
        } else if (typeStr.equals("RECORD")) {
            type = LegacySQLTypeName.RECORD;
        }

        return Field.newBuilder(nameStr, type).setMode(Field.Mode.valueOf(modeStr)).build();
    }

    public static List<Field> listToFields(List<Map> m_fields) {
        List<Field> fields = new ArrayList(m_fields.size());
        for (Map m : m_fields) {
            fields.add(mapToField(m));
        }

        return fields;
    }

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
