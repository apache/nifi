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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.ScriptRunner;
import org.apache.nifi.util.StringUtils;

import javax.script.Invocable;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains variables and methods common to scripting processors, reporting tasks, etc.
 */
public class ScriptingComponentHelper {

    public PropertyDescriptor SCRIPT_ENGINE;

    public final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public Map<String, ScriptEngineFactory> scriptEngineFactoryMap;
    private String scriptEngineName;
    private String scriptPath;
    private String scriptBody;
    private List<PropertyDescriptor> descriptors;
    private List<AllowableValue> engineAllowableValues;
    private ResourceReferences modules;

    public BlockingQueue<ScriptRunner> scriptRunnerQ = null;

    public String getScriptEngineName() {
        return scriptEngineName;
    }

    public void setScriptEngineName(String scriptEngineName) {
        this.scriptEngineName = scriptEngineName;
    }

    public String getScriptPath() {
        return scriptPath;
    }

    public void setScriptPath(String scriptPath) {
        this.scriptPath = scriptPath;
    }

    public String getScriptBody() {
        return scriptBody;
    }

    public void setScriptBody(String scriptBody) {
        this.scriptBody = scriptBody;
    }

    public String[] getModules() {
        return modules.asLocations().toArray(new String[0]);
    }

    public void setModules(final ResourceReferences modules) {
        this.modules = modules;
    }

    public List<PropertyDescriptor> getDescriptors() {
        return descriptors;
    }

    public List<AllowableValue> getScriptEngineAllowableValues() {
        return engineAllowableValues;
    }

    public void setDescriptors(List<PropertyDescriptor> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * Custom validation for ensuring exactly one of Script File or Script Body is populated
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return A collection of validation results
     */
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Verify that exactly one of "script file" or "script body" is set
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(ScriptingComponentUtils.SCRIPT_FILE)) == StringUtils.isEmpty(propertyMap.get(ScriptingComponentUtils.SCRIPT_BODY))) {
            results.add(new ValidationResult.Builder().subject("Script Body or Script File").valid(false).explanation(
                    "exactly one of Script File or Script Body must be set").build());
        }

        return results;
    }

    public void createResources() {
        createResources(true);
    }

    /**
     * This method creates all resources needed for the script processor to function, such as script engines,
     * script file reloader threads, etc.
     */
    public void createResources(final boolean requireInvocable) {
        descriptors = new ArrayList<>();
        // The following is required for JRuby, should be transparent to everything else.
        // Note this is not done in a ScriptRunner, as it is too early in the lifecycle. The
        // setting must be there before the factories/engines are loaded.
        System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");

        // Create list of available engines
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        if (scriptEngineFactories != null) {
            scriptEngineFactoryMap = new HashMap<>(scriptEngineFactories.size());
            List<AllowableValue> engineList = new LinkedList<>();
            for (ScriptEngineFactory factory : scriptEngineFactories) {
                if (!requireInvocable || factory.getScriptEngine() instanceof Invocable) {
                    engineList.add(new AllowableValue(factory.getLanguageName()));
                    scriptEngineFactoryMap.put(factory.getLanguageName(), factory);
                }
            }

            // Sort the list by name so the list always looks the same.
            engineList.sort((o1, o2) -> {
                if (o1 == null) {
                    return o2 == null ? 0 : 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.getValue().compareTo(o2.getValue());
            });

            engineAllowableValues = engineList;
            AllowableValue[] engines = engineList.toArray(new AllowableValue[0]);

            SCRIPT_ENGINE = new PropertyDescriptor.Builder()
                    .name("Script Engine")
                    .required(true)
                    .description("The engine to execute scripts")
                    .allowableValues(engines)
                    .defaultValue(engines[0].getValue())
                    .required(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .build();
            descriptors.add(SCRIPT_ENGINE);
        }

        descriptors.add(ScriptingComponentUtils.SCRIPT_FILE);
        descriptors.add(ScriptingComponentUtils.SCRIPT_BODY);
        descriptors.add(ScriptingComponentUtils.MODULES);

        isInitialized.set(true);
    }

    /**
     * Determines whether the given path refers to a valid file
     *
     * @param path a path to a file
     * @return true if the path refers to a valid file, false otherwise
     */
    public static boolean isFile(final String path) {
        return path != null && Files.isRegularFile(Paths.get(path));
    }

    public void setupScriptRunners(final int numberOfScriptEngines, final String scriptToRun, final ComponentLog log) {
        setupScriptRunners(true, numberOfScriptEngines, scriptToRun, log);
    }

    /**
     * Configures the specified script engine(s) as a queue of ScriptRunners. First, the engine is loaded and instantiated using the JSR-223
     * javax.script APIs. Then, if any script configurators have been defined for this engine, their init() method is
     * called, and the configurator is saved for future calls.
     *
     * @param numberOfScriptEngines number of engines to setup
     * @see org.apache.nifi.processors.script.ScriptRunner
     */
    public void setupScriptRunners(final boolean newQ, final int numberOfScriptEngines, final String scriptToRun, final ComponentLog log) {
        if (newQ) {
            scriptRunnerQ = new LinkedBlockingQueue<>(numberOfScriptEngines);
        }
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (StringUtils.isBlank(scriptEngineName)) {
                throw new IllegalArgumentException("The script engine name cannot be null");
            }

            // Get a list of URLs from the configurator (if present), or just convert modules from Strings to URLs
            final String[] locations = modules.asLocations().toArray(new String[0]);
            final URL[] additionalClasspathURLs = ScriptRunnerFactory.getInstance().getModuleURLsForClasspath(scriptEngineName, locations, log);


            // Need the right classloader when the engine is created. This ensures the NAR's execution class loader
            // (plus the module path) becomes the parent for the script engine
            ClassLoader scriptEngineModuleClassLoader = additionalClasspathURLs != null
                    ? new URLClassLoader(additionalClasspathURLs, originalContextClassLoader)
                    : originalContextClassLoader;
            if (scriptEngineModuleClassLoader != null) {
                Thread.currentThread().setContextClassLoader(scriptEngineModuleClassLoader);
            }

            try {
                for (int i = 0; i < numberOfScriptEngines; i++) {
                    //
                    ScriptEngineFactory factory = scriptEngineFactoryMap.get(scriptEngineName);
                    ScriptRunner scriptRunner = ScriptRunnerFactory.getInstance().createScriptRunner(factory, scriptToRun, locations);
                    if (!scriptRunnerQ.offer(scriptRunner)) {
                        log.error("Error adding script engine {}", scriptRunner.getScriptEngineName());
                    }
                }
            } catch (ScriptException se) {
                throw new ProcessException("Could not instantiate script engines", se);
            }
        } finally {
            // Restore original context class loader
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    public void setupVariables(final PropertyContext context) {
        scriptEngineName = context.getProperty(SCRIPT_ENGINE).getValue();
        scriptPath = context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).evaluateAttributeExpressions().getValue();
        scriptBody = context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue();
        modules = context.getProperty(ScriptingComponentUtils.MODULES).evaluateAttributeExpressions().asResources().flattenRecursively();
    }

    public void stop() {
        if (scriptRunnerQ != null) {
            scriptRunnerQ.clear();
        }
    }
}
