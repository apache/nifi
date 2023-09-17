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
package org.apache.nifi.script;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.ScriptRunner;
import org.apache.nifi.script.impl.ClojureScriptRunner;
import org.apache.nifi.script.impl.GenericScriptRunner;
import org.apache.nifi.script.impl.GroovyScriptRunner;
import org.apache.nifi.script.impl.JavascriptScriptRunner;
import org.apache.nifi.script.impl.JythonScriptRunner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class ScriptRunnerFactory {

    private final static ScriptRunnerFactory INSTANCE = new ScriptRunnerFactory();

    private ScriptRunnerFactory() {

    }

    public static ScriptRunnerFactory getInstance() {
        return INSTANCE;
    }

    public ScriptRunner createScriptRunner(ScriptEngineFactory scriptEngineFactory, String scriptToRun, String[] modulePaths)
            throws ScriptException {
        ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();
        String scriptEngineName = scriptEngineFactory.getLanguageName();
        if ("Groovy".equals(scriptEngineName)) {
            return new GroovyScriptRunner(scriptEngine, scriptToRun, null);
        }
        if ("python".equals(scriptEngineName)) {
            return new JythonScriptRunner(scriptEngine, scriptToRun, modulePaths);
        }
        if ("Clojure".equals(scriptEngineName)) {
            return new ClojureScriptRunner(scriptEngine, scriptToRun, null);
        }
        if ("ECMAScript".equals(scriptEngineName)) {
            return new JavascriptScriptRunner(scriptEngine, scriptToRun, null);
        }
        return new GenericScriptRunner(scriptEngine, scriptToRun, null);
    }

    /**
     * Scans the given module paths for JARs. The path itself (whether a directory or file) will be added to the list
     * of returned module URLs, and if a directory is specified, it is scanned for JAR files (files ending with .jar).
     * Any JAR files found are added to the list of module URLs. This is a convenience method for adding directories
     * full of JAR files to an ExecuteScript or InvokeScriptedProcessor instance, rather than having to enumerate each
     * JAR's URL.
     *
     * @param modulePaths An array of module paths to scan/add
     * @param log         A logger for the calling component, to provide feedback for missing files, e.g.
     * @return An array of URLs corresponding to all modules determined from the input set of module paths.
     */
    public URL[] getModuleURLsForClasspath(String scriptEngineName, String[] modulePaths, ComponentLog log) {

        if (!"Clojure".equals(scriptEngineName)
                && !"Groovy".equals(scriptEngineName)
                && !"ECMAScript".equals(scriptEngineName)) {
            return new URL[0];
        }

        List<URL> additionalClasspath = new LinkedList<>();

        if (modulePaths == null) {
            return new URL[0];
        }
        for (String modulePathString : modulePaths) {
            File modulePath = new File(modulePathString);

            if (modulePath.exists()) {
                // Add the URL of this path
                try {
                    additionalClasspath.add(modulePath.toURI().toURL());
                } catch (MalformedURLException mue) {
                    log.warn("{} is not a valid file/folder, ignoring", modulePath.getAbsolutePath(), mue);
                }

                // If the path is a directory, we need to scan for JARs and add them to the classpath
                if (!modulePath.isDirectory()) {
                    continue;
                }
                File[] jarFiles = modulePath.listFiles((dir, name) -> (name != null && name.endsWith(".jar")));

                if (jarFiles == null) {
                    continue;
                }
                // Add each to the classpath
                for (File jarFile : jarFiles) {
                    try {
                        additionalClasspath.add(jarFile.toURI().toURL());

                    } catch (MalformedURLException mue) {
                        log.warn("{} is not a valid file/folder, ignoring", modulePath.getAbsolutePath(), mue);
                    }
                }
            } else {
                log.warn("{} does not exist, ignoring", modulePath.getAbsolutePath());
            }
        }
        return additionalClasspath.toArray(new URL[0]);
    }
}
