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


import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.Map;
import java.util.Set;

public class Record {
    private JsonBuilderFactory factory = null;
    private String domain = null;
    private Set<Map.Entry<String, String>> properties = null;
    private Map<String, String> attributes = null;

    public Record(JsonBuilderFactory f) {
        this.factory = f;
    }

    public void setDomain(String d) {
        this.domain = d;
    }


    public void setProperties(Set<Map.Entry<String, String>> p) {
        this.properties = p;
    }


    public void setAttributes(Map<String, String> a) {
        this.attributes = a;
    }


    public String toString() {
        String ret = null;

        try {
            JsonArrayBuilder propertiesJsonAr = factory.createArrayBuilder();

            for (Map.Entry<String, String> entry : properties) {
                propertiesJsonAr.add(factory.createObjectBuilder()
                        .add("key", entry.getKey())
                        .add("value", entry.getValue()));
            }

            JsonArrayBuilder attributesJsonAr = factory.createArrayBuilder();

            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                attributesJsonAr.add(factory.createObjectBuilder()
                        .add("key", entry.getKey())
                        .add("value", entry.getValue()));
            }

            JsonObject json = factory.createObjectBuilder()
                    .add("domain", domain)
                    .add("properties", propertiesJsonAr.build())
                    .add("attributes", attributesJsonAr.build()).build();

            ret = json.toString();
        } catch (Exception e) {
            ret = e.toString();
        }

        return ret;
    }
}
