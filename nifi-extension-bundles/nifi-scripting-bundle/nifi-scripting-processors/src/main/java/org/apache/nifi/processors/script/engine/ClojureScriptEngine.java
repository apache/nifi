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

import clojure.lang.Compiler;
import clojure.lang.LineNumberingPushbackReader;
import clojure.lang.Namespace;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.Reader;
import java.io.StringReader;
import java.util.UUID;

/**
 * A ScriptEngine implementation for the Clojure language
 */
public class ClojureScriptEngine extends AbstractScriptEngine {

    public static final String ENGINE_NAME = "Clojure";
    public static final String ENGINE_VERSION = "1.8.0";

    private volatile ScriptEngineFactory scriptEngineFactory;
    private final String uuid = "ns-" + UUID.randomUUID().toString();
    private final Symbol NAMESPACE_SYMBOL = Symbol.create(uuid);


    protected ClojureScriptEngine(ScriptEngineFactory scriptEngineFactory) {
        this.scriptEngineFactory = scriptEngineFactory;

        // Set up the engine bindings
        Bindings engineScope = getBindings(ScriptContext.ENGINE_SCOPE);
        engineScope.put(ENGINE, ENGINE_NAME);
        engineScope.put(ENGINE_VERSION, ENGINE_VERSION);
        engineScope.put(NAME, ENGINE_NAME);
        engineScope.put(LANGUAGE, ENGINE_NAME);
        engineScope.put(LANGUAGE_VERSION, ENGINE_VERSION);
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        if (script == null) {
            throw new NullPointerException("script is null");
        }

        return eval(new StringReader(script), context);
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {

        try {
            // Get engine bindings and send them to Clojure
            Bindings engineBindings = context.getBindings(ScriptContext.ENGINE_SCOPE);
            engineBindings.entrySet().forEach((entry) -> Var.intern(Namespace.findOrCreate(NAMESPACE_SYMBOL), Symbol.create(entry.getKey().intern()), entry.getValue(), true));

            Var.pushThreadBindings(
                    RT.map(RT.CURRENT_NS, RT.CURRENT_NS.deref(),
                            RT.IN, new LineNumberingPushbackReader(context.getReader()),
                            RT.OUT, context.getWriter(),
                            RT.ERR, context.getErrorWriter()));

            Object result = Compiler.load(reader);
            return result;
        } catch (Exception e) {
            throw new ScriptException(e);
        } finally {
            Namespace.remove(NAMESPACE_SYMBOL);
        }
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return scriptEngineFactory;
    }

    public String getNamespace() {
        return uuid;
    }

}
