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
package org.apache.nifi.jms.cf;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Creates new instance of the class specified by 'className' by first
     * loading it using thread context class loader and then executing default
     * constructor.
     */
    @SuppressWarnings("unchecked")
    static <T> T newDefaultInstance(String className) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className, false, Thread.currentThread().getContextClassLoader());
            return clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load and/or instantiate class '" + className + "'", e);
        }
    }

    /**
     * Finds a method by name on the target class. If more then one method
     * present it will return the first one encountered.
     *
     * @param name        method name
     * @param targetClass instance of target class
     * @return instance of {@link Method}
     */
    public static Method findMethod(String name, Class<?> targetClass) {
        Class<?> searchType = targetClass;
        while (searchType != null) {
            Method[] methods = (searchType.isInterface() ? searchType.getMethods() : searchType.getDeclaredMethods());
            for (Method method : methods) {
                if (name.equals(method.getName())) {
                    return method;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    /**
     * Finds a method by name on the target class. If more then one method
     * present it will return the first one encountered.
     *
     * @param name        method name
     * @param targetClass instance of target class
     * @return Array of {@link Method}
     */
    public static Method[] findMethods(String name, Class<?> targetClass) {
        Class<?> searchType = targetClass;
        ArrayList<Method> fittingMethods = new ArrayList<>();
        while (searchType != null) {
            Method[] methods = (searchType.isInterface() ? searchType.getMethods() : searchType.getDeclaredMethods());
            for (Method method : methods) {
                if (name.equals(method.getName())) {
                    fittingMethods.add(method);
                }
            }
            searchType = searchType.getSuperclass();
        }
        if (fittingMethods.isEmpty()) {
            return null;
        } else {
            //Sort so that in case there are two methods that accept the parameter type
            //as first param use the one which accepts fewer parameters in total
            Collections.sort(fittingMethods, Comparator.comparing(Method::getParameterCount));
            return fittingMethods.toArray(new Method[fittingMethods.size()]);
        }
    }

}