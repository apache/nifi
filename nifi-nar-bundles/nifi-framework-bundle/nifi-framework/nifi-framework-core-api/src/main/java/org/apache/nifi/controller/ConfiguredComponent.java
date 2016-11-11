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

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;

import java.util.Collection;
import java.util.Map;

public interface ConfiguredComponent extends ComponentAuthorizable {

    public String getIdentifier();

    public String getName();

    public void setName(String name);

    public String getAnnotationData();

    public void setAnnotationData(String data);

    public void setProperties(Map<String, String> properties);

    public Map<PropertyDescriptor, String> getProperties();

    public String getProperty(final PropertyDescriptor property);

    boolean isValid();

    /**
     * @return the any validation errors for this connectable
     */
    Collection<ValidationResult> getValidationErrors();

    /**
     * @return the type of the component. I.e., the class name of the implementation
     */
    String getComponentType();

    /**
     * @return the Canonical Class Name of the component
     */
    String getCanonicalClassName();

}
