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

package org.apache.nifi.py4j.client;

import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.PythonObjectProxy;
import org.apache.nifi.python.processor.PreserveJavaBinding;
import org.apache.nifi.python.processor.PythonProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.Gateway;
import py4j.ReturnObject;
import py4j.reflection.PythonProxyHandler;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;


/**
 * <p>
 * NiFiPythonGateway is a custom extension of the Py4J Gateway class.
 * This implementation makes use of our own custom JavaObjectBindings in order to keep track of the objects
 * that are being passed between Java and Python. The Py4J implementation depends on the Python side
 * performing Garbage Collection in order to notify Java that the objects are no longer referencable and can
 * therefore be removed from Java's heap. Unfortunately, in testing this has frequently resulted in Java throwing
 * OutOfMemoryError because the Python side was not notifying Java to cleanup objects in a timely enough manner.
 * </p>
 *
 * <p>
 * We address this by employing a technique that immediately allows for garbage collection of any objects that are passed
 * to the Python side as soon as the method invocation returns. This means that the Python side is not allowed to cache
 * any objects passed to it. Given the design of the API, this is very reasonable.
 * </p>
 *
 * <p>
 * There are, however, some exceptions. For example, initialization objects are passed to the Python side and are expected
 * to be cached. In order to allow for this, we have introduced the {@link PreserveJavaBinding} annotation. If a method
 * is annotated with this annotation, then the object will not be unbound from Java's heap when the method returns.
 * Instead, it will remain bound until the Python has notified the Java side that the object has been garbage collection, in
 * the same way that the default Py4J implementation handles it.
 * </p>
 */
public class NiFiPythonGateway extends Gateway {
    private static final Logger logger = LoggerFactory.getLogger(NiFiPythonGateway.class);
    private final JavaObjectBindings objectBindings;

    // Guarded by synchronized methods
    private final List<InvocationBindings> activeInvocations = new ArrayList<>();

    private final ReturnObject END_OF_ITERATOR_OBJECT = ReturnObject.getErrorReturnObject(new NoSuchElementException());
    private final Method freeMethod;
    private final Method pingMethod;

    public NiFiPythonGateway(final JavaObjectBindings bindings, final Object entryPoint, final CallbackClient callbackClient) {
        super(entryPoint, callbackClient);
        this.objectBindings = bindings;

        freeMethod = getMethod(PythonObjectProxy.class, "free");
        pingMethod = getMethod(PythonController.class, "ping");
    }

    private Method getMethod(final Class<?> clazz, final String methodName) {
        try {
            return clazz.getMethod(methodName);
        } catch (final NoSuchMethodException ignored) {
            return null;
        }
    }

    public JavaObjectBindings getObjectBindings() {
        return objectBindings;
    }

    @Override
    public Object getObject(final String objectId) {
        return objectBindings.getBoundObject(objectId);
    }

    @Override
    public ReturnObject invoke(final String methodName, final String targetObjectId, final List<Object> args) {
        // Py4J detects the end of an iterator on the Python side by catching a Py4JError. It makes the assumption that
        // if it encounters Py4JError then it was due to a NoSuchElementException being thrown on the Java side. In this case,
        // it throws a StopIteration exception on the Python side. This has a couple of problems:
        // 1. It's not clear that the exception was thrown because the iterator was exhausted. It could have been thrown for
        //    any other reason, such as an IOException, etc.
        // 2. It relies on Exception handling for flow control, which is generally considered bad practice.
        // While we aren't going to go so far as to override the Python code, we can at least prevent Java from constantly
        // throwing the Exception, catching it, logging it, and re-throwing it.
        // Instead, we just create the ReturnObject once and return it.
        final boolean intervene = isNextOnExhaustedIterator(methodName, targetObjectId, args);
        if (!intervene) {
            return super.invoke(methodName, targetObjectId, args);
        }

        return END_OF_ITERATOR_OBJECT;
    }

    private boolean isNextOnExhaustedIterator(final String methodName, final String targetObjectId, final List<Object> args) {
        if (!"next".equals(methodName)) {
            return false;
        }
        if (args != null && !args.isEmpty()) {
            return false;
        }

        final Object targetObject = getObjectFromId(targetObjectId);
        if (!(targetObject instanceof final Iterator<?> itr)) {
            return false;
        }

        return !itr.hasNext();
    }

    @Override
    public synchronized String putNewObject(final Object object) {
        final String objectId = objectBindings.bind(object, activeInvocations.size() + 1);

        for (final InvocationBindings activeInvocation : activeInvocations) {
            activeInvocation.add(objectId);
        }

        logger.debug("Binding {}: {} ({})", objectId, object, object == null ? "null" : object.getClass().getName());
        return objectId;
    }

    @Override
    public synchronized Object putObject(final String id, final Object object) {
        objectBindings.bind(id, object, activeInvocations.size() + 1);
        logger.debug("Binding {}: {} ({})", id, object, object == null ? "null" : object.getClass().getName());

        return super.putObject(id, object);
    }

    @Override
    public void deleteObject(final String objectId) {
        logger.debug("Unbinding {} because it was explicitly requested from Python side", objectId);
        objectBindings.unbind(objectId);
    }


    @Override
    protected PythonProxyHandler createPythonProxyHandler(final String id) {
        logger.debug("Creating Python Proxy Handler for ID {}", id);
        final PythonProxyInvocationHandler createdHandler = new PythonProxyInvocationHandler(this, id);

        return new PythonProxyHandler(id, this) {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                if (Objects.equals(freeMethod, method)) {
                    createdHandler.free();
                    return null;
                }

                return createdHandler.invoke(proxy, method, args);
            }

        };
    }

    public synchronized InvocationBindings beginInvocation(final String objectId, final Method method, final Object[] args) {
        final boolean unbind = isUnbind(method);

        final InvocationBindings bindings = new InvocationBindings(objectId, method, args, unbind);

        // We don't want to keep track of invocations of the Ping method.
        // The Ping method has no arguments or bound objects to free, and can occasionally
        // even hang, if the timing is right because of the startup sequence of the Python side and how the
        // communications are established.
        if (!pingMethod.equals(method)) {
            activeInvocations.add(bindings);
        }

        logger.debug("Beginning method invocation {}", bindings);

        return bindings;
    }

    public synchronized void endInvocation(final InvocationBindings invocationBindings) {
        final String methodName = invocationBindings.getMethod().getName();
        logger.debug("Ending method invocation {}", invocationBindings);

        final String objectId = invocationBindings.getTargetObjectId();

        activeInvocations.remove(invocationBindings);
        invocationBindings.getObjectIds().forEach(id -> {
            final Object unbound = objectBindings.unbind(id);

            if (logger.isDebugEnabled()) {
                logger.debug("Unbinding {}: {} because invocation of {} on {} with args {} has completed", id, unbound, methodName, objectId, Arrays.toString(invocationBindings.getArgs()));
            }
        });
    }

    protected boolean isUnbind(final Method method) {
        final Class<?> declaringClass = method.getDeclaringClass();

        // The two main entry points into the Python side are the PythonController and PythonProcessor. When we call methods on these classes, we usually want to
        // consider the method arguments ephemeral and unbind them after the invocation has completed. However, there are some exceptions to this rule. For example,
        // when we provide an InitializationContext to a PythonProcessor, we want to keep the context bound to the Java heap until the Python side has notified us
        // that it is no longer needed. We can do this by annotating the method with the PreserveJavaBinding annotation.
        final boolean relevantClass = PythonController.class.isAssignableFrom(declaringClass) || PythonProcessor.class.isAssignableFrom(declaringClass);
        if (relevantClass && method.getAnnotation(PreserveJavaBinding.class) == null) {
            // No need for binding / unbinding if types are primitives, because the values themselves are sent across and not references to objects.
            boolean bindNecessary = isBindNecessary(method.getReturnType());
            for (final Class<?> parameterType : method.getParameterTypes()) {
                if (isBindNecessary(parameterType)) {
                    bindNecessary = true;
                    break;
                }
            }

            return bindNecessary;
        }

        return false;
    }

    private boolean isBindNecessary(final Class<?> type) {
        return !type.isPrimitive() && type != String.class && type != byte[].class;
    }


    public static class InvocationBindings {
        private final String targetObjectId;
        private final Method method;
        private final Object[] args;
        private final boolean unbind;
        private final List<String> objectIds = new ArrayList<>();

        public InvocationBindings(final String targetObjectId, final Method method, final Object[] args, final boolean unbind) {
            this.targetObjectId = targetObjectId;
            this.method = method;
            this.args = args;
            this.unbind = unbind;
        }

        public boolean isUnbind() {
            return unbind;
        }

        public void add(final String objectId) {
            objectIds.add(objectId);
        }

        public List<String> getObjectIds() {
            return objectIds;
        }

        public String getTargetObjectId() {
            return targetObjectId;
        }

        public Method getMethod() {
            return method;
        }

        public Object[] getArgs() {
            return args;
        }

        @Override
        public String toString() {
            return "InvocationBindings[method=" + method + ", target=" + targetObjectId + ", args=" + Arrays.toString(args) + "]";
        }
    }
}
