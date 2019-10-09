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
package org.apache.nifi.jms.utils;

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
    public static <T> T newDefaultInstance(String className) {
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
    private static Method findMethod(String name, Class<?> targetClass) {
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
    private static Method[] findMethods(String name, Class<?> targetClass) {
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

    /**
     * Sets corresponding {@link ConnectionFactory}'s property to a
     * 'propertyValue' by invoking a 'setter' method that corresponds to
     * 'propertyName'. For example, 'channel' property will correspond to
     * 'setChannel(..) method and 'queueManager' property will correspond to
     * setQueueManager(..) method with a single argument.
     * <p>
     * NOTE: There is a limited type conversion to accommodate property value
     * types since all NiFi configuration properties comes as String. It is
     * accomplished by checking the argument type of the method and executing
     * its corresponding conversion to target primitive (e.g., value 'true' will
     * go thru Boolean.parseBoolean(propertyValue) if method argument is of type
     * boolean). None-primitive values are not supported at the moment and will
     * result in {@link IllegalArgumentException}. It is OK though since based
     * on analysis of several ConnectionFactory implementation the all seem to
     * follow bean convention and all their properties using Java primitives as
     * arguments.
     */
    public static boolean setProperty(Object target, String propertyName, Object propertyValue) {
        String methodName = toMethodName(propertyName);
        Method[] methods = findMethods(methodName, target.getClass());
        if (methods != null && methods.length > 0) {
            try {
                for (Method method : methods) {
                    Class<?> returnType = method.getParameterTypes()[0];
                    if (String.class.isAssignableFrom(returnType)) {
                        method.invoke(target, propertyValue);
                        return true;
                    } else if (int.class.isAssignableFrom(returnType)) {
                        method.invoke(target, Integer.parseInt((String) propertyValue));
                        return true;
                    } else if (long.class.isAssignableFrom(returnType)) {
                        method.invoke(target, Long.parseLong((String) propertyValue));
                        return true;
                    } else if (boolean.class.isAssignableFrom(returnType)) {
                        method.invoke(target, Boolean.parseBoolean((String) propertyValue));
                        return true;
                    }
                }
                methods[0].invoke(target, propertyValue);
                return true;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to set property " + propertyName, e);
            }
        }
        return false;
    }

    /**
     * Will convert propertyName to a method name following bean convention. For
     * example, 'channel' property will correspond to 'setChannel method and
     * 'queueManager' property will correspond to setQueueManager method name
     */
    private static String toMethodName(String propertyName) {
        char c[] = propertyName.toCharArray();
        c[0] = Character.toUpperCase(c[0]);
        return "set" + new String(c);
    }
}