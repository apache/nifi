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

import org.apache.nifi.toolkit.cli.api.Session;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds information for the current CLI session.
 *
 * In interactive mode there will be one session object created when the CLI starts and will be used
 * across commands for the duration of the CLI.
 *
 */
public class InMemorySession implements Session {

    private static final String NIFI_CLIENT_ID = UUID.randomUUID().toString();

    private Map<String,String> variables = new ConcurrentHashMap<>();

    @Override
    public String getNiFiClientID() {
        return NIFI_CLIENT_ID;
    }

    @Override
    public void set(final String variable, final String value) {
        if (variable == null) {
            throw new IllegalArgumentException("Variable cannot be null");
        }

        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        this.variables.put(variable, value.trim());
    }

    @Override
    public String get(final String variable) {
        return this.variables.get(variable);
    }

    @Override
    public void remove(final String variable) {
        this.variables.remove(variable);
    }

    @Override
    public void clear() {
        this.variables.clear();
    }

    @Override
    public Set<String> keys() {
        return new HashSet<>(variables.keySet());
    }

    @Override
    public void printVariables(final PrintStream output) {
        output.println();
        output.println("Current Session:");
        output.println();

        for (final Map.Entry<String,String> entry : variables.entrySet()) {
            output.println(entry.getKey() + " = " + entry.getValue());
        }

        output.println();
    }
}
