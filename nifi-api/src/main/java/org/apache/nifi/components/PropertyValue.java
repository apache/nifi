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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;

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
    String getValue();

    /**
     * @return an integer representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Integer asInteger();

    /**
     * @return a Long representation of the property value, or <code>null</code>
     * if not set
     * @throws NumberFormatException if not able to parse
     */
    Long asLong();

    /**
     * @return a Boolean representation of the property value, or
     * <code>null</code> if not set
     */
    Boolean asBoolean();

    /**
     * @return a Float representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Float asFloat();

    /**
     * @return a Double representation of the property value, of
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Double asDouble();

    /**
     * @param timeUnit specifies the TimeUnit to convert the time duration into
     * @return a Long value representing the value of the configured time period
     * in terms of the specified TimeUnit; if the property is not set, returns
     * <code>null</code>
     */
    Long asTimePeriod(TimeUnit timeUnit);

    /**
     *
     * @param dataUnit specifies the DataUnit to convert the data size into
     * @return a Long value representing the value of the configured data size
     * in terms of the specified DataUnit; if hte property is not set, returns
     * <code>null</code>
     */
    Double asDataSize(DataUnit dataUnit);

    /**
     * @return the ControllerService whose identifier is the raw value of
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService
     */
    ControllerService asControllerService();

    /**
     * @param <T> the generic type of the controller service
     * @param serviceType the class of the Controller Service
     * @return the ControllerService whose identifier is the raw value of the
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService. The object returned by
     * this method is explicitly cast to type specified, if the type specified
     * is valid. Otherwise, throws an IllegalArgumentException
     *
     * @throws IllegalArgumentException if the value of <code>this</code> points
     * to a ControllerService but that service is not of type
     * <code>serviceType</code> or if <code>serviceType</code> references a
     * class that is not an interface
     */
    <T extends ControllerService> T asControllerService(Class<T> serviceType) throws IllegalArgumentException;

    /**
     * @return <code>true</code> if the user has configured a value, or if the
     * {@link PropertyDescriptor} for the associated property has a default
     * value, <code>false</code> otherwise
     */
    boolean isSet();

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language;
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @return a PropertyValue with the new value is returned, supporting call
     * chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions() throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language;
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param attributes a Map of attributes that the Expression can reference.
     * These will take precedence over any underlying variable registry values.
     *
     * @return a PropertyValue with the new value
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(Map<String, String> attributes) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language.
     * The supplied decorator is then given a chance to decorate the value, and
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param attributes a Map of attributes that the Expression can reference.
     * These will take precedence over any variables in any underlying variable
     * registry.
     * @param decorator the decorator to use in order to update the values
     * returned after variable substitution and expression language evaluation.
     *
     * @return a PropertyValue with the new value
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(Map<String, String> attributes, AttributeValueDecorator decorator) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language;
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param flowFile to evaluate attributes of. It's flow file properties and
     * then flow file attributes will take precedence over any underlying
     * variable registry.
     * @return a PropertyValue with the new value is returned, supporting call
     * chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(FlowFile flowFile) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language;
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param flowFile to evaluate attributes of. It's flow file properties and
     * then flow file attributes will take precedence over any underlying
     * variable registry.
     * @param additionalAttributes a Map of additional attributes that the
     * Expression can reference. These attributes will take precedence over any
     * conflicting attributes in the provided flowfile or any underlying
     * variable registry.
     *
     * @return a PropertyValue with the new value is returned, supporting call
     * chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language;
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param flowFile to evaluate attributes of. It's flow file properties and
     * then flow file attributes will take precedence over any underlying
     * variable registry.
     * @param additionalAttributes a Map of additional attributes that the
     * Expression can reference. These attributes will take precedence over any
     * conflicting attributes in the provided flowfile or any underlying
     * variable registry.
     * @param decorator the decorator to use in order to update the values
     * returned after variable substitution and expression language evaluation.
     *
     * @return a PropertyValue with the new value is returned, supporting call
     * chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator) throws ProcessException;


    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression
     * Language; a PropertyValue with the new value is then returned, supporting
     * call chaining.
     * </p>
     *
     * @param flowFile to evaluate attributes of
     * @param additionalAttributes a Map of additional attributes that the Expression can reference. If entries in
     * this Map conflict with entries in the FlowFile's attributes, the entries in this Map are given a higher priority.
     * @param decorator the decorator to use in order to update the values returned by the Expression Language
     * @param stateValues a Map of the state values to be referenced explicitly by specific state accessing functions
     *
     * @return a PropertyValue with the new value is returned, supporting call
     * chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or evaluating
     * the Expression against the given attributes causes an Exception to be thrown
     */
    public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator, Map<String, String> stateValues)
            throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language.
     * The supplied decorator is then given a chance to decorate the value, and
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param decorator the decorator to use in order to update the values
     * returned after variable substitution and expression language evaluation.
     * @return a PropertyValue with the new value is then returned, supporting
     * call chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(AttributeValueDecorator decorator) throws ProcessException;

    /**
     * <p>
     * Replaces values in the Property Value using the NiFi Expression Language.
     * The supplied decorator is then given a chance to decorate the value, and
     * a PropertyValue with the new value is then returned, supporting call
     * chaining. Before executing the expression language statement any
     * variables names found within any underlying {@link VariableRegistry} will
     * be substituted with their values.
     * </p>
     *
     * @param flowFile to evaluate expressions against
     * @param decorator the decorator to use in order to update the values
     * returned after variable substitution and expression language evaluation.
     *
     *
     * @return a PropertyValue with the new value is then returned, supporting
     * call chaining
     *
     * @throws ProcessException if the Expression cannot be compiled or
     * evaluating the Expression against the given attributes causes an
     * Exception to be thrown
     */
    PropertyValue evaluateAttributeExpressions(FlowFile flowFile, AttributeValueDecorator decorator) throws ProcessException;
}
