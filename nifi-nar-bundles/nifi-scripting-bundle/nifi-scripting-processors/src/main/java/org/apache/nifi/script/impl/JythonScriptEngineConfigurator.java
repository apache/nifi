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
package org.apache.nifi.script.impl;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.ScriptEngineConfigurator;
import org.python.core.PyString;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A helper class to configure the Jython engine with any specific requirements
 */
public class JythonScriptEngineConfigurator implements ScriptEngineConfigurator {

    private final AtomicReference<CompiledScript> compiledScriptRef = new AtomicReference<>();

    @Override
    public String getScriptEngineName() {
        return "python";
    }

    @Override
    public URL[] getModuleURLsForClasspath(String[] modulePaths, ComponentLog log) {
        // We don't need to add the module paths to the classpath, they will be added via sys.path.append
        return new URL[0];
    }

    @Override
    public Object init(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        // Always compile when first run
        if (engine != null && compiledScriptRef.get() == null) {
                // Add prefix for import sys and all jython modules
                String prefix = "import sys\n"
                        + Arrays.stream(modulePaths).map((modulePath) -> "sys.path.append(" + PyString.encode_UnicodeEscape(modulePath, true) + ")")
                        .collect(Collectors.joining("\n"));
                final CompiledScript compiled = ((Compilable) engine).compile(prefix + scriptBody);
                compiledScriptRef.set(compiled);
        }
        return compiledScriptRef.get();
    }

    @Override
    public Object eval(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        Object returnValue = null;
        if (engine != null) {
            final CompiledScript existing = compiledScriptRef.get();
            if (existing == null) {
                throw new ScriptException("Jython script has not been compiled, the processor must be restarted.");
            }
            returnValue = compiledScriptRef.get().eval();
        }
        return returnValue;
    }
}
