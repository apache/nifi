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

import org.apache.nifi.python.PythonController;
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
        // When the python side no longer needs an object, its finalizer will notify the Java side that it's no longer needed and can be removed
        // from the accessible objects on the Java heap. However, if we are making a call to the Python side, it's possible that even though the Python
        // side no longer needs the object, the Java side still needs it bound. For instance, consider the following case:
        //
        // Java side calls PythonController.getProcessorTypes()
        // Python side needs to return an ArrayList, so it calls back to the Java side (in a separate thread) to create this ArrayList with ID o54
        // Python side adds several Processors to the ArrayList.
        // Python side returns the ArrayList to the Java side.
        // Python side no longer needs the ArrayList, so while the Java side is processing the response from the Python side, the Python side notifies the Java side that it's no longer needed.
        // Java side unbinds the ArrayList.
        // Java side parses the response from Python, indicating that the return value is the object with ID o54.
        // Java side cannot access object with ID o54 because it was already removed.
        //
        // To avoid this, we check if there is an Invocation Binding (indicating that we are in the middle of a method invocation) and if so,
        // we add the object to a list of objects to delete on completion.
        // If there is no Invocation Binding, we unbind the object immediately.
        final InvocationBindings invocationBindings = getInvocationBindings();
        if (invocationBindings == null) {
            final Object unbound = objectBindings.unbind(objectId);
            logger.debug("Unbound {}: {} because it was explicitly requested from Python side", objectId, unbound);
        } else {
            invocationBindings.deleteOnCompletion(objectId);
            logger.debug("Unbound {} because it was requested from Python side but in active method invocation so will not remove until invocation completed", objectId);
        }
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

            invocationBindings.getObjectsToDeleteOnCompletion().forEach(id -> {
                final Object unbound = objectBindings.unbind(id);
                logger.debug("Unbinding {}: {} because invocation of {} on {} with args {} has completed", id, unbound, methodName, objectId, Arrays.toString(args));
            });

            if (Objects.equals(invocationBindings.getTargetObjectId(), objectId) && Objects.equals(invocationBindings.getMethod(), method) && Arrays.equals(invocationBindings.getArgs(), args)) {
                break;
            }
        }
    }

    protected boolean isUnbind(final Method method) {
        final Class<?> declaringClass = method.getDeclaringClass();

        // The two main entry points into the Python side are the PythonController and PythonProcessor. When we call methods on these classes, we usually want to
        // consider the method arguments ephemeral and unbind them after the invocation has completed. However, there are some exceptions to this rule. For example,
        // when we provide an InitializationContext to a PythonProcessor, we want to keep the context bound to the Java heap until the Python side has notified us
        // that it is no longer needed. We can do this by annotating the method with the PreserveJavaBinding annotation.
        final boolean relevantClass = PythonController.class.isAssignableFrom(declaringClass) || PythonProcessor.class.isAssignableFrom(declaringClass);
        if (relevantClass && method.getAnnotation(PreserveJavaBinding.class) == null) {
            return true;
        }

        return false;
    }


    private static class InvocationBindings {
        private final String targetObjectId;
        private final Method method;
        private final Object[] args;
        private final List<String> objectIds = new ArrayList<>();
        private final List<String> deleteOnCompletion = new ArrayList<>();

        public InvocationBindings(final String targetObjectId, final Method method, final Object[] args) {
            this.targetObjectId = targetObjectId;
            this.method = method;
            this.args = args;
        }

        public void add(final String objectId) {
            objectIds.add(objectId);
        }

        public void deleteOnCompletion(final String objectId) {
            deleteOnCompletion.add(objectId);
        }

        public List<String> getObjectIds() {
            return objectIds;
        }

        public List<String> getObjectsToDeleteOnCompletion() {
            return deleteOnCompletion;
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
