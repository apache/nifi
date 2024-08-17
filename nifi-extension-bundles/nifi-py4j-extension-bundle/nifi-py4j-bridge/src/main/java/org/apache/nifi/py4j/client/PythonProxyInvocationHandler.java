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

import org.apache.nifi.py4j.client.NiFiPythonGateway.InvocationBindings;
import org.apache.nifi.python.processor.Idempotent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.Protocol;
import py4j.Py4JException;
import py4j.reflection.MethodInvoker;
import py4j.reflection.TypeConverter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PythonProxyInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(PythonProxyInvocationHandler.class);

    private final String objectId;
    private final NiFiPythonGateway gateway;
    private final JavaObjectBindings bindings;
    private final String gcCommand;
    private final ConcurrentMap<Method, Object> cachedValues = new ConcurrentHashMap<>();

    public PythonProxyInvocationHandler(final NiFiPythonGateway gateway, final String objectId) {
        this.objectId = objectId;
        this.gateway = gateway;
        this.bindings = gateway.getObjectBindings();
        this.gcCommand = "g\n" + objectId + "\ne\n";
    }

    public void free() {
        if (this.objectId != Protocol.ENTRY_POINT_OBJECT_ID) {
            logger.debug("Issuing GC command to python for proxy id {}", this.objectId);
            gateway.getCallbackClient().sendCommand(gcCommand, false);
        }
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (args == null && method.getName().equals("toString")) {
            return "PythonProxy[targetObjectId=" + objectId + "]";
        }

        // Only support caching for 0-arg methods currently
        final boolean idempotent = (args == null || args.length == 0) && method.getAnnotation(Idempotent.class) != null;
        if (idempotent) {
            final Object cachedValue = cachedValues.get(method);
            if (cachedValue != null) {
                return cachedValue;
            }
        }

        final CommandBuilder commandBuilder = new CommandBuilder(bindings, objectId, method.getName());
        final String command = commandBuilder.buildCommand(args);

        if (logger.isDebugEnabled()) {
            final List<Object> argList = args == null ? Collections.emptyList() : Arrays.asList(args);
            logger.debug("Invoking {} on {} with args {} using command {}", method, proxy, argList, command);
        }

        final InvocationBindings invocationBindings = gateway.beginInvocation(this.objectId, method, args);
        try {
            final String response = gateway.getCallbackClient().sendCommand(command);
            final Object output = Protocol.getReturnValue(response, gateway);
            final Object result = convertOutput(method, output);
            if (idempotent) {
                cachedValues.putIfAbsent(method, result);
            }

            return result;
        } finally {
            if (invocationBindings.isUnbind()) {
                commandBuilder.getBoundIds().forEach(bindings::unbind);
                commandBuilder.getBoundIds().forEach(i -> logger.debug("For method invocation {} unbound {} (from command builder)", method.getName(), i));
            } else {
                commandBuilder.getBoundIds().forEach(i -> logger.debug("For method invocation {} will not unbind {} (from command builder) because arguments of this method are not to be unbound",
                    method.getName(), i));
            }

            gateway.endInvocation(invocationBindings);
        }
    }


    private Object convertOutput(final Method method, final Object output) {
        final Class<?> returnType = method.getReturnType();
        // If output is None/null or expected return type is
        // Void then return output with no conversion
        if (output == null || returnType.equals(Void.TYPE)) {
            // Do not convert void
            return output;
        }

        final Class<?> outputType = output.getClass();
        final Class<?>[] parameters = {returnType};
        final Class<?>[] arguments = {outputType};
        final List<TypeConverter> converters = new ArrayList<>();
        final int cost = MethodInvoker.buildConverters(converters, parameters, arguments);

        if (cost == -1) {
            // This will be wrapped into Py4JJavaException if the Java code is being called by Python.
            throw new Py4JException("Incompatible output type. Expected: " + returnType.getName() + " Actual: " + outputType.getName());
        }

        return converters.getFirst().convert(output);
    }

}
