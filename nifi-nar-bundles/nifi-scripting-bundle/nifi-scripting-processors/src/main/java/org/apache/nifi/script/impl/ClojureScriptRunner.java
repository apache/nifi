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

import org.apache.nifi.processors.script.engine.ClojureScriptEngine;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class ClojureScriptRunner extends BaseScriptRunner {

    private static final String PRELOADS =
            "(:import \n"
                    + "[org.apache.nifi.components "
                    + "AbstractConfigurableComponent AllowableValue ConfigurableComponent PropertyDescriptor PropertyValue ValidationContext ValidationResult Validator"
                    + "]\n"
                    + "[org.apache.nifi.components.state Scope StateManager StateMap]\n"
                    + "[org.apache.nifi.flowfile FlowFile]\n"
                    + "[org.apache.nifi.processor "
                    + "AbstractProcessor AbstractSessionFactoryProcessor DataUnit FlowFileFilter ProcessContext Processor "
                    + "ProcessorInitializationContext ProcessSession ProcessSessionFactory Relationship ProcessContext"
                    + "]\n"
                    + "[org.apache.nifi.processor.exception FlowFileAccessException FlowFileHandlingException MissingFlowFileException ProcessException]\n"
                    + "[org.apache.nifi.processor.io InputStreamCallback OutputStreamCallback StreamCallback]\n"
                    + "[org.apache.nifi.processor.util FlowFileFilters StandardValidators]\n"
                    + "[org.apache.nifi.processors.script ExecuteScript InvokeScriptedProcessor ScriptRunner]\n"
                    + "[org.apache.nifi.script ScriptingComponentHelper ScriptingComponentUtils]\n"
                    + "[org.apache.nifi.logging ComponentLog]\n"
                    + "[org.apache.nifi.lookup LookupService RecordLookupService StringLookupService LookupFailureException]\n"
                    + "[org.apache.nifi.record.sink RecordSinkService]\n"
                    + ")\n";

    public ClojureScriptRunner(ScriptEngine engine, String scriptBody, String[] modulePaths) {
        super(engine, scriptBody, modulePaths);
    }

    @Override
    public String getScriptEngineName() {
        return "Clojure";
    }

    @Override
    public void run(Bindings bindings) throws ScriptException {
        String sb = "(ns " + ((ClojureScriptEngine) scriptEngine).getNamespace() +
                " " +
                PRELOADS +
                ")\n" +
                scriptBody;
        scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        scriptEngine.eval(sb);
    }
}
