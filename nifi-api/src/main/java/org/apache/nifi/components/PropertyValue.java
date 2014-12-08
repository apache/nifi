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
package org.apache.nifi.components;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * <p>
 * A PropertyValue provides a mechanism whereby the currently configured value
 * of a processor property can be obtained in different forms.
 * </p>
 */
public interface PropertyValue {

    /**
     * @return the raw property value as a string
     */
    public String getValue();

    /**
     * @return an integer representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Integer asInteger();

    /**
     * @return a Long representation of the property value, or <code>null</code>
     * if not set
     * @throws NumberFormatException if not able to parse
     */
    public Long asLong();

    /**
     * @return a Boolean representation of the property value, or
     * <code>null</code> if not set
     */
    public Boolean asBoolean();

    /**
     * @return a Float representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Float asFloat();

    /**
     * @return a Double representation of the property value, of
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Double asDouble();

    /**
     * @param timeUnit specifies the TimeUnit to convert the time duration into
     * @return a Long value representing the value of the configured time period
     * in terms of the specified TimeUnit; if the property is not set, returns
     * <code>null</code>
     */
    public Long asTimePeriod(TimeUnit timeUnit);

    /**
     *
     * @param dataUnit specifies the DataUnit to convert the data size into
     * @return a Long value representing the value of the configured data size
     * in terms of the specified DataUnit; if hte property is not set, returns
     * <code>null</code>
     */
    public Double asDataSize(DataUnit dataUnit);

    /**
     * Returns the ControllerService whose identifier is the raw value of
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService
     *
     * @return
     */
    public ControllerService asControllerService();

    /**
     * Returns the ControllerService whose identifier is the raw value of the
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService. The object returned by
     * this method is explicitly cast to type specified, if the type specified
     * is valid. Otherwise, throws an IllegalArgumentException
     *
     * @param <T>
     * @param serviceType
     * @return
     *
     * @throws IllegalArgumentException if the value of <code>this</code> points
     * to a ControllerService but that service is not of type
     * <code>serviceType</code> or if <code>serviceType</code> references a
     * class that is not an interface
     */
    public <T extends ControllerService> T asControllerService(Class<T> serviceType) throws IllegalArgumentException;

    /**
     * Returns <code>true</code> if the user has configured a value, or if the
     * {@link PropertyDescriptor} for the associated property has a default
     * value, <code>false</code> otherwise.
     *
     * @return
     */
    public boolean isSet();

    /**
     * <p>
     * Replaces values in the Property Value using the Attribute Expression
     * Language; a PropertyValue with the new value is then returned, supporting
     * call chaining.
     * </p>
     *
     * @return
     *
     * @throws ProcessException if the Query cannot be compiled or evaluating
     * the query against the given attributes causes an Exception to be thrown
     */
    public PropertyValue evaluateAttributeExpressions() throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the Attribute Expression
     * Language; a PropertyValue with the new value is then returned, supporting
     * call chaining.
     * </p>
     *
     * @param flowFile
     * @return
     *
     * @throws ProcessException if the Query cannot be compiled or evaluating
     * the query against the given attributes causes an Exception to be thrown
     */
    public PropertyValue evaluateAttributeExpressions(FlowFile flowFile) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the Attribute Expression
     * Language. The supplied decorator is then given a chance to decorate the
     * value, and a PropertyValue with the new value is then returned,
     * supporting call chaining.
     * </p>
     *
     * @param decorator
     * @return
     *
     * @throws ProcessException if the Query cannot be compiled or evaluating
     * the query against the given attributes causes an Exception to be thrown
     */
    public PropertyValue evaluateAttributeExpressions(AttributeValueDecorator decorator) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the Attribute Expression
     * Language. The supplied decorator is then given a chance to decorate the
     * value, and a PropertyValue with the new value is then returned,
     * supporting call chaining.
     * </p>
     *
     * @param flowFile
     * @param decorator
     *
     * @return
     *
     * @throws ProcessException if the Query cannot be compiled or evaluating
     * the query against the given attributes causes an Exception to be thrown
     */
    public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, AttributeValueDecorator decorator) throws ProcessException;
}
