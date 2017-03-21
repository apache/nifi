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
package org.apache.nifi.init;

import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class is a copy of org.apache.nifi.util.ReflectionUtils. Ultimately the
 * documentation generation component should be moved to a place where it can
 * depend on this directly instead of copying it in.
 *
 *
 */
public class ReflectionUtils {

    private final static Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * Invokes all methods on the given instance that have been annotated with
     * the given annotation. If the signature of the method that is defined in
     * <code>instance</code> uses 1 or more parameters, those parameters must be
     * specified by the <code>args</code> parameter. However, if more arguments
     * are supplied by the <code>args</code> parameter than needed, the extra
     * arguments will be ignored.
     *
     * @param annotation annotation
     * @param instance instance
     * @param logger the ComponentLog to use for logging any errors. If null,
     * will use own logger, but that will not generate bulletins or easily tie
     * to the Processor's log messages.
     * @param args args
     * @return <code>true</code> if all appropriate methods were invoked and
     * returned without throwing an Exception, <code>false</code> if one of the
     * methods threw an Exception or could not be invoked; if <code>false</code>
     * is returned, an error will have been logged.
     */
    public static boolean quietlyInvokeMethodsWithAnnotation(
            final Class<? extends Annotation> annotation, final Object instance, final ComponentLog logger, final Object... args) {

        for (final Method method : instance.getClass().getMethods()) {
            if (method.isAnnotationPresent(annotation)) {

                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
                    final Class<?>[] argumentTypes = method.getParameterTypes();
                    if (argumentTypes.length > args.length) {
                        if (logger == null) {
                            LOG.error("Unable to invoke method {} on {} because method expects {} parameters but only {} were given",
                                    new Object[]{method.getName(), instance, argumentTypes.length, args.length});
                        } else {
                            logger.error("Unable to invoke method {} on {} because method expects {} parameters but only {} were given",
                                    new Object[]{method.getName(), instance, argumentTypes.length, args.length});
                        }

                        return false;
                    }

                    for (int i = 0; i < argumentTypes.length; i++) {
                        final Class<?> argType = argumentTypes[i];
                        if (!argType.isAssignableFrom(args[i].getClass())) {
                            if (logger == null) {
                                LOG.error("Unable to invoke method {} on {} because method parameter {} is expected to be of type {} but argument passed was of type {}",
                                        new Object[]{method.getName(), instance, i, argType, args[i].getClass()});
                            } else {
                                logger.error("Unable to invoke method {} on {} because method parameter {} is expected to be of type {} but argument passed was of type {}",
                                        new Object[]{method.getName(), instance, i, argType, args[i].getClass()});
                            }

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
                    } catch (final InvocationTargetException ite) {
                        if (logger == null) {
                            LOG.error("Unable to invoke method {} on {} due to {}", new Object[]{method.getName(), instance, ite.getCause()});
                            LOG.error("", ite.getCause());
                        } else {
                            logger.error("Unable to invoke method {} on {} due to {}", new Object[]{method.getName(), instance, ite.getCause()});
                        }
                    } catch (final IllegalAccessException | IllegalArgumentException t) {
                        if (logger == null) {
                            LOG.error("Unable to invoke method {} on {} due to {}", new Object[]{method.getName(), instance, t});
                            LOG.error("", t);
                        } else {
                            logger.error("Unable to invoke method {} on {} due to {}", new Object[]{method.getName(), instance, t});
                        }

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
