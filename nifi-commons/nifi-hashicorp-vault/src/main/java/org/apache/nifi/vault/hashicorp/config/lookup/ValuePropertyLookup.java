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
package org.apache.nifi.vault.hashicorp.config.lookup;

import org.apache.nifi.vault.hashicorp.HashiCorpVaultConfigurationException;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

/**
 * A simple property lookup that invokes the property descriptor to retrieve the value of the property.
 */
public class ValuePropertyLookup extends PropertyLookup {

    public ValuePropertyLookup(final PropertyDescriptor propertyDescriptor) {
        super(propertyDescriptor);
    }

    @Override
    public Object getPropertyValue(final String propertyKey, final Object obj) {
        try {
            return getPropertyDescriptor().getReadMethod().invoke(obj);
        } catch (final IllegalAccessException | InvocationTargetException e) {
            throw new HashiCorpVaultConfigurationException("Could not get the value of " + propertyKey);
        }
    }

    @Override
    public Object getEnclosingObject(Object obj) {
        return obj;
    }
}
