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
package org.apache.nifi.controller.status.history;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MetricDescriptorSerde implements JsonSerializer, JsonDeserializer {
    private final static Map<String, ValueMapper> valueMapperMap = new HashMap<>();
    private final static Map<String, ValueReducer> valueReducerMap = new HashMap<>();

    static {
        Arrays.stream(ProcessGroupStatusDescriptor.values()).forEach(descriptor -> {
            MetricDescriptor md = descriptor.getDescriptor();
            String key = createKey(md.getField(), md.getLabel(), md.getDescription());
            valueMapperMap.put(key, md.getValueFunction());
            valueReducerMap.put(key, md.getValueReducer());
        });
        Arrays.stream(ProcessorStatusDescriptor.values()).forEach(descriptor -> {
            MetricDescriptor md = descriptor.getDescriptor();
            String key = createKey(md.getField(), md.getLabel(), md.getDescription());
            valueMapperMap.put(key, md.getValueFunction());
            valueReducerMap.put(key, md.getValueReducer());
        });
        Arrays.stream(RemoteProcessGroupStatusDescriptor.values()).forEach(descriptor -> {
            MetricDescriptor md = descriptor.getDescriptor();
            String key = createKey(md.getField(), md.getLabel(), md.getDescription());
            valueMapperMap.put(key, md.getValueFunction());
            valueReducerMap.put(key, md.getValueReducer());
        });
        Arrays.stream(ConnectionStatusDescriptor.values()).forEach(descriptor -> {
            MetricDescriptor md = descriptor.getDescriptor();
            String key = createKey(md.getField(), md.getLabel(), md.getDescription());
            valueMapperMap.put(key, md.getValueFunction());
            valueReducerMap.put(key, md.getValueReducer());
        });
    }

    private static String createKey(String field, String label, String description) {
        return field + label + description;
    }

    @Override
    public JsonElement serialize(Object o, Type type, JsonSerializationContext jsonSerializationContext) {
        MetricDescriptor metricDescriptor = (MetricDescriptor) o;
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("metricIdentifier", metricDescriptor.getMetricIdentifier());
        jsonObject.addProperty("field", metricDescriptor.getField());
        jsonObject.addProperty("label", metricDescriptor.getLabel());
        jsonObject.addProperty("description", metricDescriptor.getDescription());
        jsonObject.addProperty("formatter", metricDescriptor.getFormatter().name());
        return jsonObject;
    }

    @Override
    public Object deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();
        String field = jsonObject.get("field").getAsString();
        String label = jsonObject.get("label").getAsString();
        String description = jsonObject.get("description").getAsString();
        int metricIdentifier = jsonObject.get("metricIdentifier").getAsInt();
        String formatterName = jsonObject.get("formatter").getAsString();
        MetricDescriptor.Formatter formatter = MetricDescriptor.Formatter.valueOf(formatterName);

        String key = createKey(field, label, description);

        IndexableMetric indexableMetric = () -> metricIdentifier;
        MetricDescriptor metricDescriptor = new StandardMetricDescriptor(indexableMetric, field, label, description, formatter, valueMapperMap.get(key), valueReducerMap.get(key));
        return metricDescriptor;
    }
}
