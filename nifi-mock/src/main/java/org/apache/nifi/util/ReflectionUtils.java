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
package org.apache.nifi.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionUtils {

    private final static Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * Invokes all methods on the given instance that have been annotated with
     * the given Annotation. If the signature of the method that is defined in
     * <code>instance</code> uses 1 or more parameters, those parameters must be
     * specified by the <code>args</code> parameter. However, if more arguments
     * are supplied by the <code>args</code> parameter than needed, the extra
     * arguments will be ignored.
     *
     * @param annotation
     * @param instance
     * @param args
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public static void invokeMethodsWithAnnotation(final Class<? extends Annotation> annotation, final Object instance, final Object... args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : instance.getClass().getMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
                    final Class<?>[] argumentTypes = method.getParameterTypes();
                    if (argumentTypes.length > args.length) {
                        throw new IllegalArgumentException(String.format("Unable to invoke method %1$s on %2$s because method expects %3$s parameters but only %4$s were given",
                                method.getName(), instance, argumentTypes.length, args.length));
                    }

                    for (int i = 0; i < argumentTypes.length; i++) {
                        final Class<?> argType = argumentTypes[i];
                        if (!argType.isAssignableFrom(args[i].getClass())) {
                            throw new IllegalArgumentException(String.format(
                                    "Unable to invoke method %1$s on %2$s because method parameter %3$s is expected to be of type %4$s but argument passed was of type %5$s",
                                    method.getName(), instance, i, argType, args[i].getClass()));
                        }
                    }

                    if (argumentTypes.length == args.length) {
                        method.invoke(instance, args);
                    } else {
                        final Object[] argsToPass = new Object[argumentTypes.length];
                        for (int i = 0; i < argsToPass.length; i++) {
                            argsToPass[i] = args[i];
                        }

                        method.invoke(instance, argsToPass);
                    }
                } finally {
                    if (!isAccessible) {
                        method.setAccessible(false);
                    }
                }
            }
        }
    }

    /**
     * Invokes all methods on the given instance that have been annotated with
     * the given Annotation. If the signature of the method that is defined in
     * <code>instance</code> uses 1 or more parameters, those parameters must be
     * specified by the <code>args</code> parameter. However, if more arguments
     * are supplied by the <code>args</code> parameter than needed, the extra
     * arguments will be ignored.
     *
     * @param annotation
     * @param instance
     * @param args
     * @return <code>true</code> if all appropriate methods were invoked and
     * returned without throwing an Exception, <code>false</code> if one of the
     * methods threw an Exception or could not be invoked; if <code>false</code>
     * is returned, an error will have been logged.
     */
    public static boolean quietlyInvokeMethodsWithAnnotation(final Class<? extends Annotation> annotation, final Object instance, final Object... args) {
        for (final Method method : instance.getClass().getMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
                    final Class<?>[] argumentTypes = method.getParameterTypes();
                    if (argumentTypes.length > args.length) {
                        LOG.error("Unable to invoke method {} on {} because method expects {} parameters but only {} were given",
                                new Object[]{method.getName(), instance, argumentTypes.length, args.length});
                        return false;
                    }

                    for (int i = 0; i < argumentTypes.length; i++) {
                        final Class<?> argType = argumentTypes[i];
                        if (!argType.isAssignableFrom(args[i].getClass())) {
                            LOG.error("Unable to invoke method {} on {} because method parameter {} is expected to be of type {} but argument passed was of type {}",
                                    new Object[]{method.getName(), instance, i, argType, args[i].getClass()});
                            return false;
                        }
                    }

                    try {
                        if (argumentTypes.length == args.length) {
                            method.invoke(instance, args);
                        } else {
                            final Object[] argsToPass = new Object[argumentTypes.length];
                            for (int i = 0; i < argsToPass.length; i++) {
                                argsToPass[i] = args[i];
                            }

                            method.invoke(instance, argsToPass);
                        }
                    } catch (final IllegalAccessException | IllegalArgumentException | InvocationTargetException t) {
                        LOG.error("Unable to invoke method {} on {} due to {}", new Object[]{method.getName(), instance, t});
                        LOG.error("", t);
                        return false;
                    }
                } finally {
                    if (!isAccessible) {
                        method.setAccessible(false);
                    }
                }
            }
        }
        return true;
    }
}
