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
     * Returns the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     *
     * @return
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * Returns a ValidationContext that is appropriate for validating the given
     * {@link ControllerService}
     *
     * @param controllerService
     * @return
     */
    ValidationContext getControllerServiceValidationContext(ControllerService controllerService);

    /**
     * Creates and returns a new {@link ExpressionLanguageCompiler} that can be
     * used to compile & evaluate Attribute Expressions
     *
     * @return
     */
    ExpressionLanguageCompiler newExpressionLanguageCompiler();

    /**
     * Returns a PropertyValue that encapsulates the value configured for the
     * given PropertyDescriptor
     *
     * @param property
     * @return
     */
    PropertyValue getProperty(PropertyDescriptor property);

    /**
     * Returns a PropertyValue that represents the given value
     *
     * @param value
     * @return
     */
    PropertyValue newPropertyValue(String value);

    /**
     * Returns a Map of all configured Properties.
     *
     * @return
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * Returns the currently configured Annotation Data
     *
     * @return
     */
    String getAnnotationData();
    
    /**
     * There are times when the framework needs to consider a component valid, even if it
     * references an invalid ControllerService. This method will return <code>false</code>
     * if the component is to be considered valid even if the given Controller Service is referenced
     * and is invalid.
     * @param service
     */
    boolean isValidationRequired(ControllerService service);
    
    /**
     * Returns <code>true</code> if the given value contains a NiFi Expression Language expression,
     * <code>false</code> if it does not
     * 
     * @param value
     * @return
     */
    boolean isExpressionLanguagePresent(String value);
    
    /**
     * Returns <code>true</code> if the property with the given name supports the NiFi Expression Language,
     * <code>false</code> if the property does not support the Expression Language or is not a valid property
     * name
     * 
     * @param propertyName
     * @return
     */
    boolean isExpressionLanguageSupported(String propertyName);
}
