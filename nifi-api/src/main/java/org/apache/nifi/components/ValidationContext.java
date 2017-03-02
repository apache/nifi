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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;

public interface ValidationContext {

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * @param controllerService to lookup the validation context of
     * @return a ValidationContext that is appropriate for validating the given
     * {@link ControllerService}
     */
    ValidationContext getControllerServiceValidationContext(ControllerService controllerService);

    /**
     * @return a new {@link ExpressionLanguageCompiler} that can be used to
     * compile & evaluate Attribute Expressions
     */
    ExpressionLanguageCompiler newExpressionLanguageCompiler();

    /**
     * @param property being validated
     * @return a PropertyValue that encapsulates the value configured for the
     * given PropertyDescriptor
     */
    PropertyValue getProperty(PropertyDescriptor property);

    /**
     * @param value to make a PropertyValue object for
     * @return a PropertyValue that represents the given value
     */
    PropertyValue newPropertyValue(String value);

    /**
     * @return a Map of all configured Properties
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * @return the currently configured Annotation Data
     */
    String getAnnotationData();

    /**
     * There are times when the framework needs to consider a component valid,
     * even if it references an invalid ControllerService. This method will
     * return <code>false</code> if the component is to be considered valid even
     * if the given Controller Service is referenced and is invalid.
     *
     * @param service to check if validation is required
     * @return <code>false</code> if the component is to be considered valid
     * even if the given Controller Service is referenced and is invalid
     */
    boolean isValidationRequired(ControllerService service);

    /**
     * @param value to test whether expression language is present
     * @return <code>true</code> if the given value contains a NiFi Expression
     * Language expression, <code>false</code> if it does not
     */
    boolean isExpressionLanguagePresent(String value);

    /**
     * @param propertyName to test whether expression language is supported
     * @return <code>true</code> if the property with the given name supports
     * the NiFi Expression Language, <code>false</code> if the property does not
     * support the Expression Language or is not a valid property name
     */
    boolean isExpressionLanguageSupported(String propertyName);

    /**
     * Returns the identifier of the ProcessGroup that the component being validated lives in
     *
     * @return the identifier of the ProcessGroup that the component being validated lives in
     */
    String getProcessGroupIdentifier();
}
