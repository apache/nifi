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

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.net.URL;

/**
 * A helper class to configure the Jython engine with any specific requirements
 */
public class JythonScriptEngineConfigurator implements ScriptEngineConfigurator {

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
    public Object init(ScriptEngine engine, String[] modulePaths) throws ScriptException {
        return null;
    }

    @Override
    public Object eval(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        Object returnValue = null;
        if (engine != null) {
            // Need to import the module path inside the engine, in order to pick up
            // other Python/Jython modules
            engine.eval("import sys");
            if (modulePaths != null) {
                for (String modulePath : modulePaths) {
                    engine.eval("sys.path.append('" + modulePath + "')");
                }
            }
            returnValue = engine.eval(scriptBody);
        }
        return returnValue;
    }
}
