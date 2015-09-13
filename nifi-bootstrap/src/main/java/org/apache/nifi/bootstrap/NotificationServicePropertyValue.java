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
package org.apache.nifi.bootstrap;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.FormatUtils;

public class NotificationServicePropertyValue implements PropertyValue {
    private final String rawValue;
    private final PreparedQuery preparedQuery;

    public NotificationServicePropertyValue(final String rawValue) {
        this.rawValue = rawValue;
        this.preparedQuery = Query.prepare(rawValue);
    }

    @Override
    public String getValue() {
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        return (rawValue == null) ? null : Integer.parseInt(rawValue.trim());
    }

    @Override
    public Long asLong() {
        return (rawValue == null) ? null : Long.parseLong(rawValue.trim());
    }

    @Override
    public Boolean asBoolean() {
        return (rawValue == null) ? null : Boolean.parseBoolean(rawValue.trim());
    }

    @Override
    public Float asFloat() {
        return (rawValue == null) ? null : Float.parseFloat(rawValue.trim());
    }

    @Override
    public Double asDouble() {
        return (rawValue == null) ? null : Double.parseDouble(rawValue.trim());
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.trim(), timeUnit);
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        return rawValue == null ? null : DataUnit.parseDataSize(rawValue.trim(), dataUnit);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions() throws ProcessException {
        return evaluateAttributeExpressions((AttributeValueDecorator) null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        return new NotificationServicePropertyValue(preparedQuery.evaluateExpressions((FlowFile) null, decorator));
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return rawValue;
    }

    @Override
    public ControllerService asControllerService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends ControllerService> T asControllerService(final Class<T> serviceType) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSet() {
        return rawValue != null;
    }

}
