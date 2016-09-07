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

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import javax.json.Json;
import javax.json.JsonBuilderFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class TestRecord {
    @Test
    public void validateRecord() throws Exception {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        Map<String, String> attributes = new HashMap<String, String>();

        properties.put("prop1", "propval1");
        attributes.put("attr1", "attrval1");

        JsonBuilderFactory factory = Json.createBuilderFactory(null);
        Record record = new Record(factory);

        record.setDomain("domain1");
        record.setProperties(properties.entrySet());
        record.setAttributes(attributes);

        assertTrue(record.toString().contains("domain"));
        assertTrue(record.toString().contains("properties"));
        assertTrue(record.toString().contains("attributes"));
    }
}
