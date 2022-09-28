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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.Protocol;
import py4j.Py4JException;
import py4j.StringUtil;
import py4j.reflection.MethodInvoker;
import py4j.reflection.ReflectionEngine;
import py4j.reflection.TypeConverter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PythonProxyInvocationHandler implements InvocationHandler {
    public static final String VOID_ARGUMENT_LINE = String.valueOf(Protocol.VOID);
    public static final String METHOD_INVOCATION_START_CLAUSE = "c\n";
    public static final String METHOD_INVOCATION_END_CLAUSE = "e\n";
    private static final Logger logger = LoggerFactory.getLogger(PythonProxyInvocationHandler.class);

    private final String objectId;
    private final NiFiPythonGateway gateway;
    private final JavaObjectBindings bindings;

    public PythonProxyInvocationHandler(final NiFiPythonGateway gateway, final String objectId) {
        this.objectId = objectId;
        this.gateway = gateway;
        this.bindings = gateway.getObjectBindings();
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (args == null && method.getName().equals("toString")) {
            return "PythonProxy[targetObjectId=" + objectId + "]";
        }

        final CommandBuilder commandBuilder = new CommandBuilder(bindings, objectId, method.getName());
        final String command = commandBuilder.buildCommand(args);

        if (logger.isDebugEnabled()) {
            final List<Object> argList = args == null ? Collections.emptyList() : Arrays.asList(args);
            logger.debug("Invoking {} on {} with args {} using command {}", method, proxy, argList, command);
        }

        gateway.beginInvocation(this.objectId, method, args);

        final String response = gateway.getCallbackClient().sendCommand(command);
        final Object output = Protocol.getReturnValue(response, gateway);
        final Object convertedOutput = convertOutput(method, output);

        // TODO: While we're waiting for this call to return, the Python side may call back into the Java side and create objects.
        //       When that happens we need to throw those onto the stack also!
        if (gateway.isUnbind(method)) {
            commandBuilder.getBoundIds().forEach(bindings::unbind);
            commandBuilder.getBoundIds().forEach(i -> logger.debug("For method invocation {} unbound {} (from command builder)", method.getName(), i));
        } else {
            commandBuilder.getBoundIds().forEach(i -> logger.debug("For method invocation {} will not unbind {} (from command builder) because arguments of this method are not to be unbound",
                method.getName(), i));
        }

        gateway.endInvocation(this.objectId, method, args);

        return convertedOutput;
    }


    private String buildInvocationCommand(final String methodName, final Object[] args) {
        final StringBuilder sb = new StringBuilder();
        sb.append(METHOD_INVOCATION_START_CLAUSE);
        sb.append(objectId).append("\n");
        sb.append(methodName).append("\n");

        if (args != null) {
            for (final Object arg : args) {
                final String argumentLine = getArgumentLine(arg);
                sb.append(argumentLine).append("\n");
            }
        }

        sb.append(METHOD_INVOCATION_END_CLAUSE);
        return sb.toString();
    }

    private String getArgumentLine(final Object arg) {
        if (arg == null) {
            return "n";
        }

        if (arg instanceof String || arg instanceof Character) {
            return Protocol.STRING_TYPE + StringUtil.escape(arg.toString());
        } else if (arg instanceof byte[]) {
            return Protocol.BYTES_TYPE + Protocol.encodeBytes((byte[]) arg);
        } else if (arg instanceof Long) {
            return Protocol.LONG_TYPE + arg.toString();
        } else if (arg instanceof Double || arg instanceof Float) {
            return Protocol.DOUBLE_TYPE + arg.toString();
        } else if (arg instanceof Boolean) {
            return Protocol.BOOLEAN_TYPE + arg.toString();
        } else if (arg instanceof Integer || arg instanceof Short || arg instanceof Byte) {
            return Protocol.INTEGER_TYPE + arg.toString();
        } else if (arg == ReflectionEngine.RETURN_VOID) {
            return VOID_ARGUMENT_LINE;
        } else if (arg instanceof BigDecimal) {
            return Protocol.DECIMAL_TYPE + ((BigDecimal) arg).toPlainString();
        } else if (arg instanceof List) {
            final String listId = bindings.bind(arg);
            return Protocol.LIST_TYPE + listId;
        } else if (arg instanceof Map) {
            final String mapId = bindings.bind(arg);
            return Protocol.MAP_TYPE + mapId;
        } else if (arg.getClass().isArray()) {
            final String arrayId = bindings.bind(arg);
            return Protocol.ARRAY_TYPE + arrayId;
        } else if (arg instanceof Set) {
            final String setId = bindings.bind(arg);
            return Protocol.SET_TYPE + setId;
        } else if (arg instanceof Iterator) {
            final String iteratorId = bindings.bind(arg);
            return Protocol.ITERATOR_TYPE + iteratorId;
        } else {
            final String objectId = bindings.bind(arg);
            return Protocol.REFERENCE_TYPE + objectId;
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
        final Class<?>[] parameters = { returnType };
        final Class<?>[] arguments = { outputType };
        final List<TypeConverter> converters = new ArrayList<TypeConverter>();
        final int cost = MethodInvoker.buildConverters(converters, parameters, arguments);

        if (cost == -1) {
            // This will be wrapped into Py4JJavaException if the Java code is being called by Python.
            throw new Py4JException("Incompatible output type. Expected: " + returnType.getName() + " Actual: " + outputType.getName());
        }

        return converters.get(0).convert(output);
    }

}
