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

package org.apache.nifi.hbase;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HBase_2_ListLookupService extends AbstractHBaseLookupService implements LookupService<List> {
    public static final AllowableValue KEY_LIST = new AllowableValue("key_list", "List of keys",
            "Return the row as a list of the column qualifiers (keys)");
    public static final AllowableValue VALUE_LIST = new AllowableValue("value_list", "List of values",
            "Return the row as a list of the values associated with each column qualifier.");
    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("hb-lu-list-return-type")
            .displayName("Return Type")
            .description("Choose whether to return a list of the keys or a list of the values for the supplied row key.")
            .allowableValues(KEY_LIST, VALUE_LIST)
            .defaultValue(KEY_LIST.getValue())
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final List<PropertyDescriptor> _PROPERTIES;
    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.addAll(PROPERTIES);
        _temp.add(RETURN_TYPE);
        _PROPERTIES = Collections.unmodifiableList(_temp);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return _PROPERTIES;
    }

    @Override
    public Optional<List> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates.get(ROW_KEY_KEY) == null) {
            return Optional.empty();
        }

        final String rowKey = coordinates.get(ROW_KEY_KEY).toString();
        if (StringUtils.isBlank(rowKey)) {
            return Optional.empty();
        }

        final byte[] rowKeyBytes = rowKey.getBytes(StandardCharsets.UTF_8);

        try {
            final Map<String, Object> values = scan(rowKeyBytes);

            if (values.size() > 0) {
                List<String> retVal = returnType.equals(KEY_LIST.getValue())
                        ? new ArrayList<>(values.keySet())
                        : values.values().stream().map( obj -> obj.toString() ).collect(Collectors.toList());
                return Optional.ofNullable(retVal);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            getLogger().error("Error occurred loading {}", coordinates.get("rowKey"), e);
            throw new LookupFailureException(e);
        }
    }

    private String returnType;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) throws InterruptedException, IOException, InitializationException {
        super.onEnabled(context);
        returnType = context.getProperty(RETURN_TYPE).getValue();
    }

    @Override
    public Class<?> getValueType() {
        return List.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
