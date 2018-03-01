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
package org.apache.nifi.toolkit.cli.impl.session;

import java.util.ArrayList;
import java.util.List;

/**
 * Possible variables that can be set in the session.
 */
public enum SessionVariable {

    NIFI_CLIENT_PROPS("nifi.props"),
    NIFI_REGISTRY_CLIENT_PROPS("nifi.reg.props");

    private final String variableName;

    SessionVariable(final String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return this.variableName;
    }

    public static SessionVariable fromVariableName(final String variableName) {
        for (final SessionVariable variable : values()) {
            if (variable.getVariableName().equals(variableName)) {
                return variable;
            }
        }

        return null;
    }

    public static List<String> getAllVariableNames() {
        final List<String> names = new ArrayList<>();
        for (SessionVariable variable : values()) {
            names.add(variable.getVariableName());
        }
        return names;
    }

}
