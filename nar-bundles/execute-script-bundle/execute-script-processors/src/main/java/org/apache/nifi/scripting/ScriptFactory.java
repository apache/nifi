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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.BufferedInputStream;
import org.apache.nifi.logging.ProcessorLog;

import org.apache.commons.io.FileUtils;

/**
 * While this is a 'factory', it is not a singleton because we want a factory
 * per processor. This factory has state, all of which belong to only one
 * processor.
 *
 */
public class ScriptFactory {

    private final ScriptEngineFactory engineFactory = new ScriptEngineFactory();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock readLock = lock.readLock();
    private final WriteLock writeLock = lock.writeLock();
    private final ProcessorLog logger;

    private volatile CompiledScript compiledScript;
    private volatile String scriptText;
    private volatile byte[] md5Hash;
    private volatile long lastTimeChecked;
    private volatile String scriptFileName;
    private volatile long scriptCheckIntervalMS = 15000;

    public ScriptFactory(ProcessorLog logger) {
        this.logger = logger;
    }

    public void setScriptCheckIntervalMS(long msecs) {
        this.scriptCheckIntervalMS = msecs;
    }

    /**
     * @param aScriptFileName
     * @param properties
     * @param flowFile
     * @return
     * @throws IOException
     * @throws ScriptException
     */
    public Script getScript(final String aScriptFileName, final Map<String, String> properties, final FlowFile flowFile)
            throws IOException, ScriptException {
        final Script instance;
        long now = System.currentTimeMillis();
        readLock.lock();
        try {
            if (!aScriptFileName.equals(this.scriptFileName)) {
                readLock.unlock();
                writeLock.lock();
                try {
                    if (!aScriptFileName.equals(this.scriptFileName)) {
                        // need to get brand new engine
                        compiledScript = null;
                        this.md5Hash = getMD5Hash(aScriptFileName);
                        this.lastTimeChecked = now;
                        this.scriptFileName = aScriptFileName;
                        updateEngine();
                    } // else another thread beat me to the change...so just get a script
                } finally {
                    readLock.lock();
                    writeLock.unlock();
                }
            } else if (lastTimeChecked + scriptCheckIntervalMS < now) {
                readLock.unlock();
                writeLock.lock();
                try {
                    if (lastTimeChecked + scriptCheckIntervalMS < now) {
                        byte[] md5 = getMD5Hash(this.scriptFileName);
                        if (!MessageDigest.isEqual(md5Hash, md5)) {
                            // need to get brand new engine
                            compiledScript = null;
                            updateEngine();
                            this.md5Hash = md5;
                        } // else no change to script, so just update time checked
                        this.lastTimeChecked = now;
                    } // else another thread beat me to the check...so just get a script
                } finally {
                    readLock.lock();
                    writeLock.unlock();
                }
            }
            try {
                instance = getScriptInstance(properties);
                instance.setFileName(this.scriptFileName);
                instance.setProperties(properties);
                instance.setLogger(logger);
                instance.setFlowFile(flowFile);
            } catch (ScriptException e) {
                // need to reset state to enable re-initialization
                this.lastTimeChecked = 0;
                this.scriptFileName = null;
                throw e;
            }
        } finally {
            readLock.unlock();
        }

        return instance;

    }

    public Script getScript(String aScriptFileName) throws ScriptException, IOException {
        Map<String, String> props = new HashMap<>();
        return getScript(aScriptFileName, props, null);
    }

    private byte[] getMD5Hash(String aScriptFileName) throws FileNotFoundException, IOException {
        byte[] messageDigest = null;
        try (FileInputStream fis = new FileInputStream(aScriptFileName);
                DigestInputStream dis = new DigestInputStream(new BufferedInputStream(fis), MessageDigest.getInstance("MD5"))) {

            byte[] bytes = new byte[8192];
            while (dis.read(bytes) != -1) {
                // do nothing...just computing the md5 hash
            }
            messageDigest = dis.getMessageDigest().digest();
        } catch (NoSuchAlgorithmException swallow) {
            // MD5 is a legitimate format
        }
        return messageDigest;
    }

    private String getScriptText(File scriptFile, String extension) throws IOException {
        final String script;
        switch (extension) {
            case "rb":
                script = JRubyScriptFactory.INSTANCE.getScript(scriptFile);
                break;

            case "js":
                script = JavaScriptScriptFactory.INSTANCE.getScript(scriptFile);
                break;

            case "py":
                script = JythonScriptFactory.INSTANCE.getScript(scriptFile);
                break;

            default:
                script = FileUtils.readFileToString(scriptFile);
        }
        return script;
    }

    private Script getScriptInstance(final Map<String, String> properties) throws ScriptException {

        Map<String, Object> localThreadVariables = new HashMap<>();
        final String extension = getExtension(scriptFileName);
        String loggerVariableKey = getVariableName("GLOBAL", "logger", extension);
        localThreadVariables.put(loggerVariableKey, logger);
        String propertiesVariableKey = getVariableName("INSTANCE", "properties", extension);
        localThreadVariables.put(propertiesVariableKey, properties);
        localThreadVariables.put(ScriptEngine.FILENAME, scriptFileName);
        final Bindings bindings = new SimpleBindings(localThreadVariables);
        final ScriptEngine scriptEngine = engineFactory.getEngine(extension);
        Script instance;
        if (compiledScript == null) {
            instance = (Script) scriptEngine.eval(scriptText, bindings);
            if (instance == null) { // which it will be for python and also for local variables in javascript
                instance = (Script) scriptEngine.eval("instance", bindings);
            }
        } else {
            instance = (Script) compiledScript.eval(bindings);
            if (instance == null) { // which it will be for python and also for local variables in javascript
                instance = (Script) compiledScript.getEngine().eval("instance", bindings);
            }
        }
        instance.setEngine(scriptEngine);
        return instance;
    }

    /*
     * Must have writeLock when calling this!!!!
     */
    private void updateEngine() throws IOException, ScriptException {
        final String extension = getExtension(scriptFileName);
        // if engine is thread safe, it's being reused...if it's a JrubyEngine it
        File scriptFile = new File(this.scriptFileName);
        ScriptEngine scriptEngine = engineFactory.getNewEngine(scriptFile, extension);
        scriptText = getScriptText(scriptFile, extension);
        Map<String, Object> localThreadVariables = new HashMap<>();
        String loggerVariableKey = getVariableName("GLOBAL", "logger", extension);
        localThreadVariables.put(loggerVariableKey, logger);
        String propertiesVariableKey = getVariableName("INSTANCE", "properties", extension);
        localThreadVariables.put(propertiesVariableKey, new HashMap<String, String>());
        localThreadVariables.put(ScriptEngine.FILENAME, scriptFileName);
        if (scriptEngine instanceof Compilable) {
            Bindings bindings = new SimpleBindings(localThreadVariables);
            scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
            compiledScript = ((Compilable) scriptEngine).compile(scriptText);
        }
        logger.debug("Updating Engine!!");
    }

    private String getVariableName(String scope, String variableName, String extension) {
        String result;
        switch (extension) {
            case "rb":
                switch (scope) {
                    case "GLOBAL":
                        result = '$' + variableName;
                        break;
                    case "INSTANCE":
                        result = '@' + variableName;
                        break;
                    default:
                        result = variableName;
                        break;
                }

                break;

            default:
                result = variableName;
                break;
        }
        return result;
    }

    private String getExtension(String aScriptFileName) {
        int dotPos = aScriptFileName.lastIndexOf('.');
        if (dotPos < 1) {
            throw new IllegalArgumentException("Script file name must have an extension");
        }
        final String extension = aScriptFileName.substring(dotPos + 1);
        return extension;
    }
}
