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

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * This class offers methods to perform operations during the script runner lifecycle.
 */
public class GenericScriptRunner extends BaseScriptRunner {

    private String engineName = "Unknown";

    public GenericScriptRunner(ScriptEngine engine, String scriptBody, String[] modulePaths) {
        super(engine, scriptBody, modulePaths);
        this.engineName = engine.getFactory().getEngineName();
    }

    @Override
    public String getScriptEngineName() {
        return engineName;
    }

    @Override
    public void run(Bindings bindings) throws ScriptException {
        scriptEngine.eval(scriptBody);
    }
}
