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
package org.apache.nifi.scripting;

import java.io.InputStream;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.apache.nifi.processor.Relationship;

/**
 * <p>
 * Script authors should extend this class if they want to follow the "reader"
 * paradigm for NiFi processors.
 * </p>
 *
 * <p>
 * User scripts should implement {@link #route(InputStream)}. <code>route</code>
 * uses a returned relationship name to determine where FlowFiles go. Scripts
 * may also implement {@link #getProcessorRelationships()} to specify available
 * relationship names.
 * </p>
 *
 */
public class ReaderScript extends Script {

    private Object routeCallback;

    public ReaderScript(Object... callbacks) {
        super(callbacks);
        for (Object callback : callbacks) {
            if (callback instanceof Map<?, ?>) {
                routeCallback = routeCallback == null && ((Map<?, ?>) callback).containsKey("route") ? callback : routeCallback;
            }
        }
    }

    public ReaderScript() {

    }

    // Simple helper
    public void process(InputStream input) throws NoSuchMethodException, ScriptException {
        lastRoute = route(input);
    }

    /**
     * Subclasses should examine the provided inputstream, then determine which
     * relationship the file will be sent down and return its name.
     *
     *
     * @param in a Java InputStream containing the incoming FlowFile.
     * @return a relationship name
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public Relationship route(InputStream in) throws NoSuchMethodException, ScriptException {
        Relationship relationship = null;
        Invocable invocable = (Invocable) this.engine;
        relationship = (Relationship) invocable.invokeMethod(routeCallback, "route", in);
        return relationship;
    }
}
