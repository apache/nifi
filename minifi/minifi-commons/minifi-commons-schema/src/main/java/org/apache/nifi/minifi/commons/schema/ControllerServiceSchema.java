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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;

import java.util.Map;
import java.util.TreeMap;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ANNOTATION_DATA_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.TYPE_KEY;

public class ControllerServiceSchema extends BaseSchemaWithIdAndName {

    private Map<String, Object> properties = DEFAULT_PROPERTIES;
    private String annotationData = "";
    private String serviceClass;

    public ControllerServiceSchema(Map map) {
        super(map, "Controller Service(id: {id}, name: {name})");
        String wrapperName = getWrapperName();
        serviceClass = getRequiredKeyAsType(map, TYPE_KEY, String.class, wrapperName);
        properties = getOptionalKeyAsType(map, PROPERTIES_KEY, Map.class, wrapperName, DEFAULT_PROPERTIES);
        annotationData = getOptionalKeyAsType(map, ANNOTATION_DATA_KEY, String.class, wrapperName, "");
    }


    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(TYPE_KEY, serviceClass);
        result.put(PROPERTIES_KEY, new TreeMap<>(properties));

        if(annotationData != null && !annotationData.isEmpty()) {
            result.put(ANNOTATION_DATA_KEY, annotationData);
        }

        return result;
    }

    public String getAnnotationData() {
        return annotationData;
    }

    public String getServiceClass() {
        return serviceClass;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
