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
package org.apache.nifi.mock;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockConfigurationContext implements ConfigurationContext {

    @Override
    public PropertyValue getProperty(PropertyDescriptor property) {
        return null;
    }

    @Override
    public Map<String, String> getAllProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String getSchedulingPeriod() {
        return "0 secs";
    }

    @Override
    public Long getSchedulingPeriod(final TimeUnit timeUnit) {
        return 0L;
    }
}
