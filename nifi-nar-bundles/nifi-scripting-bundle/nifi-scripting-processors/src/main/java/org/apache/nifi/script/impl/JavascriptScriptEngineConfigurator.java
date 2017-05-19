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

import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * This class offers methods to perform Javascript-specific operations during the script engine lifecycle.
 */
public class JavascriptScriptEngineConfigurator extends AbstractModuleClassloaderConfigurator {

    @Override
    public String getScriptEngineName() {
        return "ECMAScript";
    }

    @Override
    public Object init(ScriptEngine engine, String[] modulePaths) throws ScriptException {
        // No initialization methods needed at present
        return engine;
    }

    @Override
    public Object eval(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        return engine.eval(scriptBody);
    }
}
