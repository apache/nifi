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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;

public abstract class DateTimeTextRecordSetWriter extends SchemaRegistryRecordSetWriter {

    private volatile Optional<String> dateFormat;
    private volatile Optional<String> timeFormat;
    private volatile Optional<String> timestampFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @OnEnabled
    public void captureValues(final ConfigurationContext context) {
        this.dateFormat = Optional.ofNullable(context.getProperty(DateTimeUtils.DATE_FORMAT).getValue());
        this.timeFormat = Optional.ofNullable(context.getProperty(DateTimeUtils.TIME_FORMAT).getValue());
        this.timestampFormat = Optional.ofNullable(context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue());
    }

    protected Optional<String> getDateFormat() {
        return dateFormat;
    }

    protected Optional<String> getTimeFormat() {
        return timeFormat;
    }

    protected Optional<String> getTimestampFormat() {
        return timestampFormat;
    }
}
