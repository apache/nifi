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
package org.apache.nifi.processors.script.engine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A ScriptEngineFactory implementation for the Clojure language
 */
public class ClojureScriptEngineFactory implements ScriptEngineFactory {

    public static final List<String> EXTENSIONS = Collections.unmodifiableList(Collections.singletonList("clj"));
    public static final List<String> MIME_TYPES = Collections.unmodifiableList(Arrays.asList("application/clojure", "text/clojure"));
    public static final List<String> NAMES = Collections.unmodifiableList(Collections.singletonList("Clojure"));

    // This engine provides constants, global engine properties, etc. to be returned by the ScriptEngineFactory interface methods,
    // and is not used to execute scripts. A new ClojureScriptEngine will be returned by each call to getScriptEngine()
    private static ScriptEngine scriptEngine;

    public ClojureScriptEngineFactory() {
        scriptEngine = getScriptEngine();
    }

    @Override
    public String getEngineName() {
        return (String) scriptEngine.get(ScriptEngine.ENGINE);
    }

    @Override
    public String getEngineVersion() {
        return (String) scriptEngine.get(ScriptEngine.ENGINE_VERSION);
    }

    @Override
    public List<String> getExtensions() {
        return EXTENSIONS;
    }

    @Override
    public List<String> getMimeTypes() {
        return MIME_TYPES;
    }

    @Override
    public List<String> getNames() {
        return NAMES;
    }

    @Override
    public String getLanguageName() {
        return (String) scriptEngine.get(ScriptEngine.LANGUAGE);
    }

    @Override
    public String getLanguageVersion() {
        return (String) scriptEngine.get(ScriptEngine.LANGUAGE_VERSION);
    }

    @Override
    public Object getParameter(String key) {
        return key == null ? null : scriptEngine.get(key);
    }

    @Override
    public String getMethodCallSyntax(String object, String method, String... args) {
        // construct a statement like (.method object arg1 arg2 ...). This works for instance methods as well as statics
        List<String> params = Arrays.asList("(." + method, object);
        params.addAll(Arrays.asList(args));
        return params.stream().collect(Collectors.joining(" ")).concat(")");
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        return toDisplay == null ? null : "(println \"" + toDisplay + "\")";
    }

    @Override
    public String getProgram(String... statements) {
        if (statements == null) {
            return null;
        }
        return Arrays.stream(statements).collect(Collectors.joining("\""));
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return new ClojureScriptEngine(this);
    }
}
