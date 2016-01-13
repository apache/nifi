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
package org.apache.nifi.controller;

import java.util.Collection;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;

public interface ConfiguredComponent {

    public String getIdentifier();

    public String getName();

    public void setName(String name);

    public String getAnnotationData();

    public void setAnnotationData(String data);

    /**
     * Sets the property with the given name to the given value
     *
     * @param name the name of the property to update
     * @param value the value to update the property to
     * @param triggerOnPropertyModified if <code>true</code>, will trigger the #onPropertyModified method of the component
     *            to be called, otherwise will not
     */
    public void setProperty(String name, String value, boolean triggerOnPropertyModified);

    /**
     * Removes the property and value for the given property name if a
     * descriptor and value exists for the given name. If the property is
     * optional its value might be reset to default or will be removed entirely
     * if was a dynamic property.
     *
     * @param name the property to remove
     * @param triggerOnPropertyModified if <code>true</code>, will trigger the #onPropertyModified method of the component
     *            to be called, otherwise will not
     * @return true if removed; false otherwise
     * @throws java.lang.IllegalArgumentException if the name is null
     */
    public boolean removeProperty(String name, boolean triggerOnPropertyModified);

    public Map<PropertyDescriptor, String> getProperties();

    public String getProperty(final PropertyDescriptor property);

    boolean isValid();

    /**
     * @return the any validation errors for this connectable
     */
    Collection<ValidationResult> getValidationErrors();
}
