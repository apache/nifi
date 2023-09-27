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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.nifi.python.processor.PreserveJavaBinding;
import org.apache.nifi.python.processor.PythonProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.Gateway;
import py4j.reflection.PythonProxyHandler;


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
    private final Map<Long, Stack<InvocationBindings>> invocationBindingsById = new ConcurrentHashMap<>();

    public NiFiPythonGateway(final JavaObjectBindings bindings, final Object entryPoint, final CallbackClient callbackClient) {
        super(entryPoint, callbackClient);
        this.objectBindings = bindings;
    }


    public JavaObjectBindings getObjectBindings() {
        return objectBindings;
    }

    @Override
    public Object getObject(final String objectId) {
        return objectBindings.getBoundObject(objectId);
    }

    @Override
    public String putNewObject(final Object object) {
        final String objectId = objectBindings.bind(object);

        final InvocationBindings bindings = getInvocationBindings();
        if (bindings != null) {
            bindings.add(objectId);
        }
        logger.debug("Binding {}: {} ({}) for {}", objectId, object, object == null ? "null" : object.getClass().getName(), bindings);
        return objectId;
    }

    @Override
    public Object putObject(final String id, final Object object) {
        objectBindings.bind(id, object);
        logger.debug("Binding {}: {} ({})", id, object, object == null ? "null" : object.getClass().getName());

        return super.putObject(id, object);
    }

    @Override
    public void deleteObject(final String objectId) {
        final Object unbound = objectBindings.unbind(objectId);
        logger.debug("Unbound {}: {} because it was explicitly requested from Python side", objectId, unbound);
    }

    private InvocationBindings getInvocationBindings() {
        final long threadId = Thread.currentThread().threadId();
        final Stack<InvocationBindings> stack = invocationBindingsById.get(threadId);
        if (stack == null || stack.isEmpty()) {
            return null;
        }

        return stack.peek();
    }

    @Override
    protected PythonProxyHandler createPythonProxyHandler(final String id) {
        logger.debug("Creating Python Proxy Handler for ID {}", id);
        final PythonProxyInvocationHandler createdHandler = new PythonProxyInvocationHandler(this, id);
        return new PythonProxyHandler(id, this) {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                return createdHandler.invoke(proxy, method, args);
            }

            @Override
            protected void finalize() {
                // Do nothing. Prevent super.finalize() from being called.
            }
        };
    }

    public void beginInvocation(final String objectId, final Method method, final Object[] args) {
        final long threadId = Thread.currentThread().threadId();
        final InvocationBindings bindings = new InvocationBindings(objectId, method, args);
        final Stack<InvocationBindings> stack = invocationBindingsById.computeIfAbsent(threadId, id -> new Stack<>());
        stack.push(bindings);

        logger.debug("Beginning method invocation {} on {} with args {}", method, objectId, Arrays.toString(args));
    }

    public void endInvocation(final String objectId, final Method method, final Object[] args) {
        final boolean unbind = isUnbind(method);

        final long threadId = Thread.currentThread().threadId();
        final Stack<InvocationBindings> stack = invocationBindingsById.get(threadId);
        if (stack == null) {
            return;
        }

        while (!stack.isEmpty()) {
            final InvocationBindings invocationBindings = stack.pop();
            final String methodName = invocationBindings.getMethod().getName();

            invocationBindings.getObjectIds().forEach(id -> {
                if (unbind) {
                    final Object unbound = objectBindings.unbind(id);
                    logger.debug("Unbinding {}: {} because invocation of {} on {} with args {} has completed", id, unbound, methodName, objectId, Arrays.toString(args));
                } else {
                    logger.debug("Will not unbind {} even though invocation of {} on {} with args {} has completed because of the method being completed",
                        id, methodName, objectId, Arrays.toString(args));
                }
            });

            if (Objects.equals(invocationBindings.getTargetObjectId(), objectId) && Objects.equals(invocationBindings.getMethod(), method) && Arrays.equals(invocationBindings.getArgs(), args)) {
                break;
            }
        }
    }

    protected boolean isUnbind(final Method method) {
        final Class<?> declaringClass = method.getDeclaringClass();
        if (PythonProcessor.class.isAssignableFrom(declaringClass) && method.getAnnotation(PreserveJavaBinding.class) == null) {
            return true;
        }

        return false;
    }


    private static class InvocationBindings {
        private final String targetObjectId;
        private final Method method;
        private final Object[] args;
        private final List<String> objectIds = new ArrayList<>();

        public InvocationBindings(final String targetObjectId, final Method method, final Object[] args) {
            this.targetObjectId = targetObjectId;
            this.method = method;
            this.args = args;
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
