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
package org.apache.nifi.processors.script.impl;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.nifi.processors.script.ScriptEngineConfigurator;
import org.apache.nifi.stream.io.ByteArrayInputStream;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

public class GroovyScriptEngineConfigurator implements ScriptEngineConfigurator, Compilable {

    private static final String PRELOADS =
            "import org.apache.nifi.components.*\n"
                    + "import org.apache.nifi.flowfile.FlowFile\n"
                    + "import org.apache.nifi.processor.*\n"
                    + "import org.apache.nifi.processor.exception.*\n"
                    + "import org.apache.nifi.processor.io.*\n"
                    + "import org.apache.nifi.processor.util.*\n"
                    + "import org.apache.nifi.processors.script.*\n"
                    + "import org.apache.nifi.logging.ProcessorLog\n";

    private ScriptEngine scriptEngine;

    @Override
    public String getScriptEngineName() {
        return "Groovy";
    }

    @Override
    public Object init(ScriptEngine engine, String modulePath) throws ScriptException {
        scriptEngine = engine;
        return scriptEngine;
    }

    @Override
    public Object eval(ScriptEngine engine, InputStream scriptStream, String modulePath) throws ScriptException {
        scriptEngine = engine;
        try {
            return engine.eval(getSequenceStreamReader(scriptStream));
        } catch (UnsupportedEncodingException uee) {
            throw new ScriptException(uee);
        }
    }

    @Override
    public CompiledScript compile(String script) throws ScriptException {
        return this.compile(new StringReader(script));
    }

    @Override
    public CompiledScript compile(Reader script) throws ScriptException {
        if (scriptEngine == null) {
            throw new ScriptException("No script engine defined!");
        }
        if (!(scriptEngine instanceof Compilable)) {
            throw new ScriptException("Script engine is not compilable!");
        }

        try {
            return ((Compilable) scriptEngine).compile(getSequenceStreamReader(new ReaderInputStream(script)));
        } catch (UnsupportedEncodingException uee) {
            throw new ScriptException(uee);
        }
    }

    private Reader getSequenceStreamReader(InputStream scriptBodyStream) throws UnsupportedEncodingException {
        ByteArrayInputStream bais = new ByteArrayInputStream(PRELOADS.getBytes("UTF-8"));
        SequenceInputStream fullInputStream = new SequenceInputStream(bais, scriptBodyStream);
        return new InputStreamReader(fullInputStream);

    }
}
