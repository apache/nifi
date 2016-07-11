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

package org.apache.nifi.serialization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.serialization.DataTypeValidator;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public abstract class UserTypeOverrideRowReader extends AbstractControllerService {
    private volatile Map<String, DataType> fieldTypeOverrides;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .dynamic(true)
            .addValidator(new DataTypeValidator())
            .build();
    }

    @OnEnabled
    public void createFieldTypeOverrides(final ConfigurationContext context) {
        final Map<String, DataType> overrides = new HashMap<>(context.getProperties().size());
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String fieldName = entry.getKey().getName();
            final String dataTypeName = entry.getValue();
            if (dataTypeName == null) {
                continue;
            }

            final DataType dataType;
            final String[] splits = dataTypeName.split("\\:");
            if (splits.length == 2) {
                final RecordFieldType fieldType = RecordFieldType.of(splits[0]);
                final String format = splits[1];
                dataType = fieldType.getDataType(format);
            } else {
                final RecordFieldType fieldType = RecordFieldType.of(dataTypeName);
                dataType = fieldType.getDataType();
            }

            overrides.put(fieldName, dataType);
        }

        this.fieldTypeOverrides = Collections.unmodifiableMap(overrides);
    }

    protected Map<String, DataType> getFieldTypeOverrides() {
        return fieldTypeOverrides;
    }
}
