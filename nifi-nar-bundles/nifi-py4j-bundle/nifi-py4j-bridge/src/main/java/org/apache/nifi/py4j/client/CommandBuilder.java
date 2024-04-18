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

import py4j.Protocol;
import py4j.StringUtil;
import py4j.reflection.ReflectionEngine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CommandBuilder {
    private static final String VOID_ARGUMENT_LINE = String.valueOf(Protocol.VOID);
    public static final String METHOD_INVOCATION_START_CLAUSE = "c\n";
    public static final String METHOD_INVOCATION_END_CLAUSE = "e\n";

    private final JavaObjectBindings bindings;
    private final List<String> boundIds = new ArrayList<>();
    private final String objectId;
    private final String methodName;

    public CommandBuilder(final JavaObjectBindings bindings, final String objectId, final String methodName) {
        this.bindings = bindings;
        this.objectId = objectId;
        this.methodName = methodName;
    }

    public String buildCommand(final Object[] args) {
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
            final String listId = bind(arg);
            return Protocol.LIST_TYPE + listId;
        } else if (arg instanceof Map) {
            final String mapId = bind(arg);
            return Protocol.MAP_TYPE + mapId;
        } else if (arg.getClass().isArray()) {
            final String arrayId = bind(arg);
            return Protocol.ARRAY_TYPE + arrayId;
        } else if (arg instanceof Set) {
            final String setId = bind(arg);
            return Protocol.SET_TYPE + setId;
        } else if (arg instanceof Iterator) {
            final String iteratorId = bind(arg);
            return Protocol.ITERATOR_TYPE + iteratorId;
        } else {
            final String objectId = bind(arg);
            return Protocol.REFERENCE_TYPE + objectId;
        }
    }

    private String bind(final Object value) {
        final String objectId = bindings.bind(value, 1);
        boundIds.add(objectId);
        return objectId;
    }

    public List<String> getBoundIds() {
        return boundIds;
    }

    public String toString() {
        return "CommandBuilder[objectId=" + objectId + ", methodName=" + methodName + ", boundIds=" + boundIds.size() + "]";
    }
}
