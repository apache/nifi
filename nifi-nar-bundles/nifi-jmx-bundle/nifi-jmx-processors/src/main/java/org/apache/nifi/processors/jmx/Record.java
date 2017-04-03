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

package org.apache.nifi.processors.jmx;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class Record {
    private String domain = null;
    private Set<Map.Entry<String, String>> properties = null;
    private Map<String, String> attributes = null;

    public void setDomain(String d) {
        this.domain = d;
    }

    public void setProperties(Set<Map.Entry<String, String>> p) {
        this.properties = p;
    }

    public void setAttributes(Map<String, String> a) {
        this.attributes = a;
    }


    public String toJsonString() {
        JsonData jsonData = new JsonData();

        ArrayList<KeyValue> keyValues = new ArrayList<>();

        for (Map.Entry<String, String> entry : properties) {
            KeyValue kv = new KeyValue(entry.getKey(), entry.getValue());
            keyValues.add(kv);
        }

        jsonData.setProperties(keyValues);

        keyValues = new ArrayList<>();

        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            KeyValue kv = new KeyValue(entry.getKey(), entry.getValue());
            keyValues.add(kv);
        }

        jsonData.setAttributes(keyValues);

        jsonData.setDomain(domain);

        Gson jsonObject = new Gson();

        return jsonObject.toJson(jsonData);
    }


    private class JsonData {
        ArrayList<KeyValue> properties;

        ArrayList<KeyValue> attributes;

        String domain;

        ArrayList<KeyValue> getProperties() {
            return properties;
        }

        void setProperties(ArrayList<KeyValue> properties) {
            this.properties = properties;
        }

        ArrayList<KeyValue> getAttributes() {
            return attributes;
        }

        void setAttributes(ArrayList<KeyValue> attributes) {
            this.attributes = attributes;
        }

        String getDomain() {
            return domain;
        }

        void setDomain(String domain) {
            this.domain = domain;
        }
    }


    private class KeyValue {
        String key = null;
        String value = null;

        KeyValue(String k, String v) {
            this.key = k;
            this.value = v;
        }

        String getKey() {
            return key;
        }

        void setKey(String key) {
            this.key = key;
        }

        String getValue() {
            return value;
        }

        void setValue(String value) {
            this.value = value;
        }
    }
}
