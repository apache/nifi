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
package org.apache.nifi.processors.script;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

class PythonScriptEvaluator implements ScriptEvaluator {
    private final ScriptEngine scriptEngine;
    private final CompiledScript compiledScript;
    private final Bindings bindings;

    PythonScriptEvaluator(
        final ScriptEngine scriptEngine,
        final CompiledScript compiledScript,
        final FlowFile flowFile,
        final ComponentLog componentLog
    ) {
        // By pre-compiling the script here, we get significant performance gains. A quick 5-minute benchmark
        // shows gains of about 100x better performance. But even with the compiled script, performance pales
        // in comparison with Groovy.
        this.compiledScript = compiledScript;
        this.scriptEngine = scriptEngine;
        this.bindings = ScriptedTransformRecord.setupBindings(scriptEngine);

        bindings.put("attributes", flowFile.getAttributes());
        bindings.put("log", componentLog);
    }

    @Override
    public Object evaluate(final Record record, final long index) throws ScriptException {
        bindings.put("record", record);
        bindings.put("recordIndex", index);

        compiledScript.eval(bindings);
        return scriptEngine.get("_");
    }
}
