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
package org.apache.nifi.vault.hashicorp.config;

import org.apache.nifi.vault.hashicorp.HashiCorpVaultConfigurationException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HashiCorpVaultPropertyMapper {
    public static final String SEPARATOR = ".";
    public static final String GET = "get";
    private final Map<String, Method> propertyKeyMap;
    private final Map<Method, HashiCorpVaultPropertyMapper> nestedPropertyMappers;

    /**
     * Maps the properties in a given class to their field names (e.g., prefix.propertyName)
     * @param prefix An optional field key prefix to use
     * @param beanClass A class to inspect for properties
     */
    public HashiCorpVaultPropertyMapper(final String prefix, final Class<?> beanClass) {
        propertyKeyMap = new HashMap<>();
        nestedPropertyMappers = new HashMap<>();

        Arrays.stream(beanClass.getDeclaredMethods())
                .filter(m -> m.getAnnotation(HashiCorpVaultProperty.class) != null)
                .forEach(m -> {
                    final String propertyKey = getPropertyKey(prefix, m.getName());
                    if (m.getReturnType().equals(String.class)) {
                        propertyKeyMap.put(propertyKey, m);
                    } else {
                        nestedPropertyMappers.put(m, new HashiCorpVaultPropertyMapper(propertyKey, m.getReturnType()));
                    }
                });
    }

    public Object getPropertyValue(final String propertyKey, final Object obj) {
        if (propertyKeyMap.containsKey(propertyKey)) {
            try {
                return propertyKeyMap.get(propertyKey).invoke(obj);
            } catch (final IllegalAccessException | InvocationTargetException e) {
                throw new HashiCorpVaultConfigurationException("Could not get the value of " + propertyKey);
            }
        }
        for(final Map.Entry<Method, HashiCorpVaultPropertyMapper> entry : nestedPropertyMappers.entrySet()) {
            try {
                final HashiCorpVaultPropertyMapper propertyMapper = entry.getValue();
                final Method getterMethod = entry.getKey();
                final Object nestedObject = getterMethod.invoke(obj);

                final Object result = propertyMapper.getPropertyValue(propertyKey, nestedObject);
                if (result != null) {
                    return result;
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new HashiCorpVaultConfigurationException("Could not get the value of " + propertyKey);
            }
        }
        return null;
    }

    private static String getPropertyName(final String getterName) {
        return getterName.substring(GET.length()).substring(0, 1).toLowerCase() + getterName.substring(GET.length() + 1);
    }

    private static String getPropertyKey(final String prefix, final String getterName) {
        return prefix == null ? getPropertyName(getterName) : String.join(SEPARATOR, prefix, getPropertyName(getterName));
    }
}
