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
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperty;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A property lookup that indexes the properties of a Java bean.
 */
public class BeanPropertyLookup extends PropertyLookup {
    private static final String SEPARATOR = ".";

    private final Map<String, PropertyLookup> propertyLookupMap;

    public BeanPropertyLookup(final String prefix, final Class<?> beanClass) {
        this(prefix, beanClass, null);
    }

    private BeanPropertyLookup(final String prefix, final Class<?> beanClass, final PropertyDescriptor propertyDescriptor) {
        super(propertyDescriptor);
        propertyLookupMap = Arrays.stream(BeanUtils.getPropertyDescriptors(beanClass))
                .filter(pd -> pd.getReadMethod().getAnnotation(HashiCorpVaultProperty.class) != null)
                .collect(Collectors.toMap(
                        pd -> getPropertyKey(prefix, pd),
                        pd -> pd.getReadMethod().getReturnType().equals(String.class) ? new ValuePropertyLookup(pd)
                                : new BeanPropertyLookup(getPropertyKey(prefix, pd), pd.getReadMethod().getReturnType(), pd)
                ));
    }

    private static String getPropertyKey(final String prefix, final PropertyDescriptor propertyDescriptor) {
        final HashiCorpVaultProperty propertyAnnotation = propertyDescriptor.getReadMethod().getAnnotation(HashiCorpVaultProperty.class);
        final String unqualifiedPropertyKey = !propertyAnnotation.key().isEmpty() ? propertyAnnotation.key() : propertyDescriptor.getDisplayName();
        return prefix == null ? unqualifiedPropertyKey: String.join(SEPARATOR, prefix, unqualifiedPropertyKey);
    }

    @Override
    public Object getPropertyValue(final String propertyKey, final Object obj) {
        if (propertyLookupMap.containsKey(propertyKey)) {
            final PropertyLookup propertyLookup = propertyLookupMap.get(propertyKey);
            return propertyLookup.getPropertyValue(propertyKey, propertyLookup.getEnclosingObject(obj));
        }
        for(final Map.Entry<String, PropertyLookup> entry : propertyLookupMap.entrySet()) {
            final String key = entry.getKey();
            if (propertyKey.startsWith(key + SEPARATOR)) {
                final PropertyLookup propertyLookup = entry.getValue();
                return propertyLookup.getPropertyValue(propertyKey, propertyLookup.getEnclosingObject(obj));
            }
        }
        return null;
    }

    @Override
    public Object getEnclosingObject(Object obj) {
        try {
            return getPropertyDescriptor().getReadMethod().invoke(obj);
        } catch (final IllegalAccessException | InvocationTargetException e) {
            throw new HashiCorpVaultConfigurationException("Could not invoke " + getPropertyDescriptor().getDisplayName());
        }
    }
}
