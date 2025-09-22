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

import org.apache.nifi.processors.script.ScriptRunner;
import org.apache.nifi.util.StringUtils;

import javax.script.ScriptEngine;

/**
 * This base class provides a common implementation for the member variables underlying the
 * ScriptRunner interface
 */
public abstract class BaseScriptRunner implements ScriptRunner {

    protected ScriptEngine scriptEngine;
    protected String scriptBody;
    protected String[] modulePaths;

    public BaseScriptRunner(final ScriptEngine engine, final String scriptBody, final String[] modulePaths) {
        this(engine, scriptBody, null, modulePaths);
    }

    public BaseScriptRunner(final ScriptEngine engine, final String scriptBody, final String preloads, final String[] modulePaths) {
        this.scriptEngine = engine;
        this.modulePaths = modulePaths;
        if (StringUtils.isNotEmpty(preloads)) {
            this.scriptBody = preloads + scriptBody;
        } else {
            this.scriptBody = scriptBody;
        }
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return scriptEngine;
    }

    @Override
    public String getScript() {
        return scriptBody;
    }
}
