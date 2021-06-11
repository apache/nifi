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

import org.python.core.PyString;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * A helper class to configure the Jython engine with any specific requirements
 */
public class JythonScriptRunner extends BaseScriptRunner {

    private final CompiledScript compiledScript;

    public JythonScriptRunner(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        super(engine, scriptBody, modulePaths);
        // Add prefix for import sys and all jython modules
        String prefix = "import sys\n"
                + Arrays.stream(modulePaths).map((modulePath) -> "sys.path.append(" + PyString.encode_UnicodeEscape(modulePath, true) + ")")
                .collect(Collectors.joining("\n"));
        compiledScript = ((Compilable) engine).compile(prefix + scriptBody);
    }

    @Override
    public String getScriptEngineName() {
        return "python";
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return scriptEngine;
    }

    @Override
    public void run(Bindings bindings) throws ScriptException {
        if (compiledScript == null) {
            throw new ScriptException("Jython script has not been successfully compiled");
        }
        compiledScript.eval();
    }
}
