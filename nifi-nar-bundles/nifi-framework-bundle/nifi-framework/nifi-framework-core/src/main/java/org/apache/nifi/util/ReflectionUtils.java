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
import java.util.Arrays;

import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

public class ReflectionUtils {

    private final static Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * Invokes all methods on the given instance that have been annotated with the given Annotation. If the signature of the method that is defined in <code>instance</code> uses 1 or more parameters,
     * those parameters must be specified by the <code>args</code> parameter. However, if more arguments are supplied by the <code>args</code> parameter than needed, the extra arguments will be
     * ignored.
     *
     * @param annotation annotation
     * @param instance instance
     * @param args args
     * @throws InvocationTargetException ex
     * @throws IllegalArgumentException ex
     * @throws IllegalAccessException ex
     */
    public static void invokeMethodsWithAnnotation(
            final Class<? extends Annotation> annotation, final Object instance, final Object... args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        invokeMethodsWithAnnotations(annotation, null, instance, args);
    }

    /**
     * Invokes all methods on the given instance that have been annotated with the given preferredAnnotation and if no such method exists will invoke all methods on the given instance that have been
     * annotated with the given alternateAnnotation, if any exists. If the signature of the method that is defined in <code>instance</code> uses 1 or more parameters, those parameters must be
     * specified by the <code>args</code> parameter. However, if more arguments are supplied by the <code>args</code> parameter than needed, the extra arguments will be ignored.
     *
     * @param preferredAnnotation preferred
     * @param alternateAnnotation alternate
     * @param instance instance
     * @param args args
     * @throws InvocationTargetException ex
     * @throws IllegalArgumentException ex
     * @throws IllegalAccessException ex
     */
    @SuppressWarnings("unchecked")
    public static void invokeMethodsWithAnnotations(final Class<? extends Annotation> preferredAnnotation,
            final Class<? extends Annotation> alternateAnnotation, final Object instance, final Object... args)
                    throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Class<? extends Annotation>[] annotationArray = (Class<? extends Annotation>[]) (alternateAnnotation != null
                ? new Class<?>[] { preferredAnnotation, alternateAnnotation } : new Class<?>[] { preferredAnnotation });
        invokeMethodsWithAnnotations(false, null, instance, annotationArray, args);
    }

    /**
     * Invokes all methods on the given instance that have been annotated with the given Annotation. If the signature of the method that is defined in <code>instance</code> uses 1 or more parameters,
     * those parameters must be specified by the <code>args</code> parameter. However, if more arguments are supplied by the <code>args</code> parameter than needed, the extra arguments will be
     * ignored.
     *
     * @param annotation annotation
     * @param instance instance
     * @param args args
     * @return <code>true</code> if all appropriate methods were invoked and returned without throwing an Exception, <code>false</code> if one of the methods threw an Exception or could not be
     * invoked; if <code>false</code> is returned, an error will have been logged.
     */
    public static boolean quietlyInvokeMethodsWithAnnotation(final Class<? extends Annotation> annotation, final Object instance, final Object... args) {
        return quietlyInvokeMethodsWithAnnotations(annotation, null, instance, null, args);
    }

    /**
     * Invokes all methods on the given instance that have been annotated with the given Annotation. If the signature of the method that is defined in <code>instance</code> uses 1 or more parameters,
     * those parameters must be specified by the <code>args</code> parameter. However, if more arguments are supplied by the <code>args</code> parameter than needed, the extra arguments will be
     * ignored.
     *
     * @param annotation annotation
     * @param instance instance
     * @param logger logger
     * @param args args
     * @return <code>true</code> if all appropriate methods were invoked and returned without throwing an Exception, <code>false</code> if one of the methods threw an Exception or could not be
     * invoked; if <code>false</code> is returned, an error will have been logged.
     */
    public static boolean quietlyInvokeMethodsWithAnnotation(final Class<? extends Annotation> annotation,
            final Object instance, final ComponentLog logger, final Object... args) {
        return quietlyInvokeMethodsWithAnnotations(annotation, null, instance, logger, args);
    }

    /**
     * Invokes all methods on the given instance that have been annotated with the given preferredAnnotation and if no such method exists will invoke all methods on the given instance that have been
     * annotated with the given alternateAnnotation, if any exists. If the signature of the method that is defined in <code>instance</code> uses 1 or more parameters, those parameters must be
     * specified by the <code>args</code> parameter. However, if more arguments are supplied by the <code>args</code> parameter than needed, the extra arguments will be ignored.
     *
     * @param preferredAnnotation preferred
     * @param alternateAnnotation alternate
     * @param instance instance
     * @param args args
     * @return <code>true</code> if all appropriate methods were invoked and returned without throwing an Exception, <code>false</code> if one of the methods threw an Exception or could not be
     * invoked; if <code>false</code> is returned, an error will have been logged.
     */
    public static boolean quietlyInvokeMethodsWithAnnotations(final Class<? extends Annotation> preferredAnnotation,
            final Class<? extends Annotation> alternateAnnotation, final Object instance, final Object... args) {
        return quietlyInvokeMethodsWithAnnotations(preferredAnnotation, alternateAnnotation, instance, null, args);
    }

    private static boolean invokeMethodsWithAnnotations(boolean quietly, ComponentLog logger, Object instance,
            Class<? extends Annotation>[] annotations, Object... args)
                    throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return invokeMethodsWithAnnotations(quietly, logger, instance, instance.getClass(), annotations, args);
    }

    private static boolean invokeMethodsWithAnnotations(boolean quietly, ComponentLog logger, Object instance,
            Class<?> clazz, Class<? extends Annotation>[] annotations, Object... args)
                    throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        boolean isSuccess = true;
        for (Method method : clazz.getMethods()) {
            if (isAnyAnnotationPresent(method, annotations)) {
                Object[] modifiedArgs = buildUpdatedArgumentsList(quietly, method, annotations, logger, args);
                if (modifiedArgs != null) {
                    try {
                        method.invoke(instance, modifiedArgs);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                        isSuccess = false;
                        if (quietly) {
                            logErrorMessage("Failed while invoking annotated method '" + method + "' with arguments '"
                                    + Arrays.asList(modifiedArgs) + "'.", logger, e);
                        } else {
                            throw e;
                        }
                    }
                }
            }
        }
        return isSuccess;
    }

    private static boolean isAnyAnnotationPresent(Method method, Class<? extends Annotation>[] annotations) {
        for (Class<? extends Annotation> annotation : annotations) {
            if (AnnotationUtils.findAnnotation(method, annotation) != null) {
                return true;
            }
        }
        return false;
    }

    private static Object[] buildUpdatedArgumentsList(boolean quietly, Method method, Class<?>[] annotations, ComponentLog processLogger, Object... args) {
        boolean parametersCompatible = true;
        int argsCount = 0;

        Class<?>[] paramTypes = method.getParameterTypes();
        for (int i = 0; parametersCompatible && i < paramTypes.length && i < args.length; i++) {
            if (paramTypes[i].isAssignableFrom(args[i].getClass())) {
                argsCount++;
            } else {
                logErrorMessage("Can not invoke method '" + method + "' with provided arguments since argument " + i + " of type '" + paramTypes[i]
                        + "' is not assignable from provided value of type '" + args[i].getClass() + "'.", processLogger, null);
                if (quietly){
                    parametersCompatible = false;
                } else {
                    argsCount++;
                }
            }
        }

        Object[] updatedArguments = null;
        if (parametersCompatible) {
            updatedArguments = Arrays.copyOf(args, argsCount);
        }
        return updatedArguments;
    }

    private static void logErrorMessage(String message, ComponentLog processLogger, Exception e) {
        if (processLogger != null) {
            if (e != null) {
                processLogger.error(message, e);
            } else {
                processLogger.error(message);
            }
        } else {
            if (e != null) {
                LOG.error(message, e);
            } else {
                LOG.error(message);
            }
        }
    }

    /**
     * Invokes all methods on the given instance that have been annotated with
     * the given preferredAnnotation and if no such method exists will invoke
     * all methods on the given instance that have been annotated with the given
     * alternateAnnotation, if any exists. If the signature of the method that
     * is defined in <code>instance</code> uses 1 or more parameters, those
     * parameters must be specified by the <code>args</code> parameter. However,
     * if more arguments are supplied by the <code>args</code> parameter than
     * needed, the extra arguments will be ignored.
     *
     * @param preferredAnnotation preferred
     * @param alternateAnnotation alternate
     * @param instance instance
     * @param logger the ComponentLog to use for logging any errors. If null, will
     *            use own logger, but that will not generate bulletins or easily
     *            tie to the Processor's log messages.
     * @param args args
     * @return <code>true</code> if all appropriate methods were invoked and
     *         returned without throwing an Exception, <code>false</code> if one
     *         of the methods threw an Exception or could not be invoked; if
     *         <code>false</code> is returned, an error will have been logged.
     */
    @SuppressWarnings("unchecked")
    public static boolean quietlyInvokeMethodsWithAnnotations(final Class<? extends Annotation> preferredAnnotation,
            final Class<? extends Annotation> alternateAnnotation, final Object instance, final ComponentLog logger,
            final Object... args) {
        Class<? extends Annotation>[] annotationArray = (Class<? extends Annotation>[]) (alternateAnnotation != null
                ? new Class<?>[] { preferredAnnotation, alternateAnnotation } : new Class<?>[] { preferredAnnotation });
        try {
            return invokeMethodsWithAnnotations(true, logger, instance, annotationArray, args);
        } catch (Exception e) {
            LOG.error("Failed while attempting to invoke methods with '" + Arrays.asList(annotationArray) + "' annotations", e);
            return false;
        }
    }
}
