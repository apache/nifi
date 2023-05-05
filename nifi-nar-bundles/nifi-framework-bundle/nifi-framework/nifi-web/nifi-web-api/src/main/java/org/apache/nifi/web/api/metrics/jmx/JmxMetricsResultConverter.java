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
package org.apache.nifi.web.api.metrics.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JmxMetricsResultConverter {
    private static final String COMPOSITE_DATA_KEY = "CompositeData%s";

    public Object convert(final Object attributeValue) {
        if (attributeValue instanceof CompositeData[]) {
            final CompositeData[] valueArray = (CompositeData[]) attributeValue;
            final Map<String, Object> values = new LinkedHashMap<>();

            for (int i = 0; i < valueArray.length; i++) {
                final Map<String, Object> subValues = new LinkedHashMap<>();
                convertCompositeData(valueArray[i], subValues);
                values.put(String.format(COMPOSITE_DATA_KEY, i), subValues);
            }
            return values;
        } else if (attributeValue instanceof CompositeData) {
            final Map<String, Object> values = new LinkedHashMap<>();
            convertCompositeData(((CompositeData) attributeValue), values);
            return values;
        } else if (attributeValue instanceof TabularData) {
            final Map<String, Object> values = new LinkedHashMap<>();
            convertTabularData((TabularData) attributeValue, values);
            return values;
        } else {
            return attributeValue;
        }
    }

    private void convertCompositeData(CompositeData attributeValue, Map<String, Object> values) {
        for (String key : attributeValue.getCompositeType().keySet()) {
            values.put(key, convert(attributeValue.get(key)));
        }
    }

    private void convertTabularData(TabularData attributeValue, Map<String, Object> values) {
        final Set<List<?>> keys = (Set<List<?>>) attributeValue.keySet();
        for (List<?> key : keys) {
            Object value = convert(attributeValue.get(key.toArray()));
            values.put(key.toString(), value);
        }
    }
}
