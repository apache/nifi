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
package org.apache.nifi.reporting.azure.loganalytics;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAzureLogAnalyticsProvenanceReportingTask {

    @Test
    public void testAddField1() {

        final Map<String, Object> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyString", "StringValue", true);
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyInteger", 2674440, true);
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyLong", 1289904147324L, true);
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyBoolean", true, true);
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyNotSupportedObject", 1.25, true);
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, "TestKeyNull", null, true);
        javax.json.JsonObject actualJson = builder.build();
        String expectedjsonString = "{" +
                                        "\"TestKeyString\": \"StringValue\"," +
                                        "\"TestKeyInteger\": 2674440," +
                                        "\"TestKeyLong\": 1289904147324," +
                                        "\"TestKeyBoolean\": true," +
                                        "\"TestKeyNotSupportedObject\": \"1.25\"," +
                                        "\"TestKeyNull\": null" +
                                    "}";
        JsonObject expectedJson = new Gson().fromJson(expectedjsonString, JsonObject.class);
        assertEquals(expectedJson.toString(), actualJson.toString());
    }

    @Test
    public void testAddField2() {

        final Map<String, Object> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        Map<String, String> values = new LinkedHashMap<>();
        values.put("TestKeyString1", "StringValue1");
        values.put("TestKeyString2", "StringValue2");
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, factory, "TestKeyString", values, true);
        javax.json.JsonObject actualJson = builder.build();
        String expectedjsonString = "{\"TestKeyString\":{\"TestKeyString1\":\"StringValue1\",\"TestKeyString2\":\"StringValue2\"}}";
        JsonObject expectedJson = new Gson().fromJson(expectedjsonString, JsonObject.class);
        assertEquals(expectedJson.toString(), actualJson.toString());
    }

    @Test
    public void testAddField3() {

        final Map<String, Object> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        Collection<String> values = new ArrayList<>();
        values.add("TestValueString1");
        values.add("TestValueString2");
        AzureLogAnalyticsProvenanceReportingTask.addField(builder, factory, "TestKeyString", values, true);
        javax.json.JsonObject actualJson = builder.build();
        String expectedjsonString = "{\"TestKeyString\":[\"TestValueString1\",\"TestValueString2\"]}";
        JsonObject expectedJson = new Gson().fromJson(expectedjsonString, JsonObject.class);
        assertEquals(expectedJson.toString(), actualJson.toString());
    }
}