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
package org.apache.nifi.scripting;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang3.StringUtils;
import org.jruby.embed.PropertyName;

public class ScriptEngineFactory {

    private static final String THREADING = "THREADING";
    private static final String MULTITHREADED = "MULTITHREADED";
    private static final String STATELESS = "STATELESS";
    private static final String THREAD_ISOLATED = "THREAD-ISOLATED";
    final static ScriptEngineManager scriptEngMgr;

    static {
        System.setProperty(PropertyName.LOCALCONTEXT_SCOPE.toString(), "singlethread");
        System.setProperty(PropertyName.COMPILEMODE.toString(), "jit");
        System.setProperty(PropertyName.COMPATVERSION.toString(), "JRuby1.9");
        System.setProperty(PropertyName.LOCALVARIABLE_BEHAVIOR.toString(), "transient");
        System.setProperty("compile.invokedynamic", "false");
        System.setProperty(PropertyName.LAZINESS.toString(), "true");
        scriptEngMgr = new ScriptEngineManager();
    }
    final ConcurrentHashMap<String, ScriptEngine> threadSafeEngines = new ConcurrentHashMap<>();

    ScriptEngine getEngine(String extension) {
        ScriptEngine engine = threadSafeEngines.get(extension);
        if (null == engine) {
            engine = scriptEngMgr.getEngineByExtension(extension);
            if (null == engine) {
                throw new IllegalArgumentException("No ScriptEngine exists for extension " + extension);
            }

            Object threading = engine.getFactory().getParameter(THREADING);
            // the MULTITHREADED status means that the scripts need to be careful about sharing state
            if (THREAD_ISOLATED.equals(threading) || STATELESS.equals(threading) || MULTITHREADED.equals(threading)) {
                ScriptEngine cachedEngine = threadSafeEngines.putIfAbsent(extension, engine);
                if (null != cachedEngine) {
                    engine = cachedEngine;
                }
            }
        }
        return engine;
    }

    ScriptEngine getNewEngine(File scriptFile, String extension) throws ScriptException {
        ScriptEngine engine = scriptEngMgr.getEngineByExtension(extension);
        if (null == engine) {
            throw new IllegalArgumentException("No ScriptEngine exists for extension " + extension);
        }
        // Initialize some paths
        StringBuilder sb = new StringBuilder();
        switch (extension) {
            case "rb":
                String parent = scriptFile.getParent();
                parent = StringUtils.replace(parent, "\\", "/");
                sb.append("$:.unshift '")
                        .append(parent)
                        .append("'\n")
                        .append("$:.unshift File.join '")
                        .append(parent)
                        .append("', 'lib'\n");
                engine.eval(sb.toString());

                break;
            case "py":
                parent = scriptFile.getParent();
                parent = StringUtils.replace(parent, "\\", "/");
                String lib = parent + "/lib";
                sb.append("import sys\n").append("sys.path.append('").append(parent)
                        .append("')\n").append("sys.path.append('")
                        .append(lib)
                        .append("')\n")
                        .append("__file__ = '")
                        .append(scriptFile.getAbsolutePath())
                        .append("'\n");
                engine.eval(sb.toString());
                break;
            default:
                break;
        }

        Object threading = engine.getFactory().getParameter(THREADING);
        // the MULTITHREADED status means that the scripts need to be careful about sharing state
        if (THREAD_ISOLATED.equals(threading) || STATELESS.equals(threading) || MULTITHREADED.equals(threading)) {
            // replace prior instance if any
            threadSafeEngines.put(extension, engine);
        }
        return engine;
    }

    boolean isThreadSafe(String scriptExtension) {
        return threadSafeEngines.containsKey(scriptExtension);
    }
}
