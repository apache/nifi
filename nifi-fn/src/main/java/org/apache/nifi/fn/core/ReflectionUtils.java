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
package org.apache.nifi.fn.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.util.NiFiProperties;
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
     * @param annotation the annotation to look for
     * @param instance to invoke a method of
     * @param args to supply in a method call
     * @throws InvocationTargetException ite
     * @throws IllegalArgumentException iae
     * @throws IllegalAccessException if not allowed to invoke that method
     */
    public static void invokeMethodsWithAnnotation(final Class<? extends Annotation> annotation, final Object instance, final Object... args)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
     * @param annotation the annotation to look for
     * @param instance to invoke a method of
     * @param args to supply in a method call
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
    public static ControllerService createControllerService(VersionedControllerService versionedControllerService) {
        //org.apache.nifi.registry.flow.Bundle bundle = versionedControllerService.getBundle();
        //BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), "1.7.1");
        //final Bundle processorBundle = ExtensionManager.getBundle(coordinate);
        //if (processorBundle == null) {
        //    throw new ProcessorInstantiationException("Unable to find bundle for coordinate " + bundle.toString());
        //}

        final Bundle systemBundle = SystemBundle.create(new NiFiProperties() {
            @Override
            public String getProperty(String s) {
                if(s.equals("nifi.nar.library.directory"))
                    return "/usr/share/nifi-1.8.0/lib/";
                return null;
            }

            @Override
            public Set<String> getPropertyKeys() {
                return null;
            }
        });
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            //final ClassLoader detectedClassLoaderForInstance = ExtensionManager.createInstanceClassLoader(versionedControllerService.getType(), UUID.randomUUID().toString(), systemBundle, null);
            final Class<?> rawClass = Class.forName(versionedControllerService.getType(), true, ctxClassLoader);
            //Thread.currentThread().setContextClassLoader(detectedClassLoaderForInstance);

            final Class<? extends ControllerService> processorClass = rawClass.asSubclass(ControllerService.class);
            return processorClass.newInstance();
        } catch (final Throwable t) {
            throw new ControllerServiceInstantiationException(versionedControllerService.getType(), t);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }
    public static Processor createProcessor(VersionedProcessor versionedProcessor) throws ProcessorInstantiationException {
        //org.apache.nifi.registry.flow.Bundle bundle = versionedProcessor.getBundle();
        //BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), "1.8.0");
        //final Bundle processorBundle = ExtensionManager.getBundle(coordinate);
        //if (processorBundle == null) {
        //    throw new ProcessorInstantiationException("Unable to find bundle for coordinate " + bundle.toString());
        //}

        final Bundle systemBundle = SystemBundle.create(new NiFiProperties() {
            @Override
            public String getProperty(String s) {
                if(s.equals("nifi.nar.library.directory"))
                    return "/usr/share/nifi-1.8.0/lib/";
                return null;
            }

            @Override
            public Set<String> getPropertyKeys() {
                return null;
            }
        });
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            //final ClassLoader detectedClassLoaderForInstance = ExtensionManager.createInstanceClassLoader(versionedProcessor.getType(), UUID.randomUUID().toString(), systemBundle, null);
            final Class<?> rawClass = Class.forName(versionedProcessor.getType(), true, ctxClassLoader);
            //Thread.currentThread().setContextClassLoader(detectedClassLoaderForInstance);

            final Class<? extends Processor> processorClass = rawClass.asSubclass(Processor.class);
            return processorClass.newInstance();
        } catch (final Throwable t) {
            throw new ProcessorInstantiationException(versionedProcessor.getType(), t);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }
}
