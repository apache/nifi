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

package org.apache.nifi.components.connector;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StandardConnectorPropertyValue implements ConnectorPropertyValue {

    private final String rawValue;

    public StandardConnectorPropertyValue(final String rawValue) {
        this.rawValue = rawValue;
    }

    @Override
    public String getValue() {
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        return rawValue == null ? null : Integer.parseInt(rawValue);
    }

    @Override
    public Long asLong() {
        return rawValue == null ? null : Long.parseLong(rawValue);
    }

    @Override
    public Boolean asBoolean() {
        return rawValue == null ? null : Boolean.parseBoolean(rawValue);
    }

    @Override
    public Float asFloat() {
        return rawValue == null ? null : Float.parseFloat(rawValue);
    }

    @Override
    public Double asDouble() {
        return rawValue == null ? null : Double.parseDouble(rawValue);
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return rawValue == null ? null : FormatUtils.getTimeDuration(rawValue, timeUnit);
    }

    @Override
    public Duration asDuration() {
        return rawValue == null ? null : Duration.ofNanos(asTimePeriod(TimeUnit.NANOSECONDS));
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        return rawValue == null ? null : DataUnit.parseDataSize(rawValue.trim(), dataUnit);
    }

    @Override
    public List<String> asList() {
        if (rawValue == null) {
            return Collections.emptyList();
        }

        final String[] splits = rawValue.split(",");
        final List<String> values = new ArrayList<>(splits.length);
        for (final String split : splits) {
            values.add(split.trim());
        }

        return values;
    }

    @Override
    public boolean isSet() {
        return rawValue != null;
    }
}
