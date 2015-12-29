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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"script", "execute", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "scala"})
@CapabilityDescription("Executes a script")
@DynamicProperty(
        name = "A script engine property to update",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value "
                + "specified by the Dynamic Property's value")
@SeeAlso({})
public class ExecuteScript extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that were failed to process")
            .build();

    public static final Relationship REL_NOFLOWFILE = new Relationship.Builder()
            .name("no-flowfile")
            .description("The script did not return a FlowFile")
            .build();

    public static PropertyDescriptor SCRIPT_ENGINE;

    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to script file to execute. Use either file or body not both")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("Script Body")
            .required(false)
            .description("Body to script to execute. Use either file or body not both")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SCRIPT_ARGS = new PropertyDescriptor.Builder()
            .name("Arguments")
            .required(false)
            .description("Arguments to pass to scripting engine")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(true)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("Module Directory")
            .description("Path to a directory which contains modules required by the script script.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(new StandardValidators.DirectoryExistsValidator(true, false))
            .build();

    // A map from engine name to a custom configurator for that engine
    private static final Map<String, ScriptEngineConfigurator>
            scriptEngineConfiguratorMap = new ConcurrentHashMap<>();

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private SynchronousFileWatcher scriptWatcher;

    private Map<String, ScriptEngineFactory> scriptEngineFactoryMap;
    private ScriptEngine scriptEngine;
    private String scriptEngineName;
    private String scriptPath;
    private String scriptBody;
    private String modulePath;
    private CompiledScript compiledScript;
    private AtomicBoolean scriptNeedsReload = new AtomicBoolean(true);
    private ScheduledExecutorService reloadService;
    private List<PropertyDescriptor> descriptors;


    /**
     * Initializes this processor. A reload service is defined and scheduled, for the purpose of watching for
     * script file changes, which indicates a reload is necessary
     *
     * @param context in which to perform initialization
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {
    }


    protected void createResources() {

        // Set up script file reloader service. This checks to see if the script file has changed, and if so, marks
        // the script file as needing a reload before evaluation
        if (reloadService == null) {
            reloadService = Executors.newScheduledThreadPool(1);

            // monitor the script if configured for changes
            reloadService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        final boolean hasLock = lock.tryLock();

                        // if a property is changing we don't need to reload this iteration
                        if (hasLock) {
                            try {
                                if (scriptWatcher != null && scriptWatcher.checkAndReset()) {
                                    if (isFile(scriptPath)) {
                                        scriptNeedsReload.set(true);
                                    }
                                }
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (final Throwable t) {
                        final ProcessorLog logger = getLogger();
                        final String message = "Unable to reload configured script Processor: " + t;

                        logger.error(message);
                        if (logger.isDebugEnabled()) {
                            logger.error(message, t);
                        }
                    }
                }
            }, 30, 10, TimeUnit.SECONDS);
        }

        descriptors = new ArrayList<>();

        // The following is required for JRuby, should be transparent to everything else.
        // Note this is not done in a ScriptEngineConfigurator, as it is too early in the lifecycle. The
        // setting must be there before the factories/engines are loaded.
        System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");

        // Create list of available engines
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        if (scriptEngineFactories != null) {
            scriptEngineFactoryMap = new HashMap<>(scriptEngineFactories.size());
            List<AllowableValue> engineList = new LinkedList<>();
            for (ScriptEngineFactory factory : scriptEngineFactories) {
                engineList.add(new AllowableValue(factory.getLanguageName()));
                scriptEngineFactoryMap.put(factory.getLanguageName(), factory);
            }

            // Sort the list by name so the list always looks the same.
            Collections.sort(engineList, new Comparator<AllowableValue>() {
                @Override
                public int compare(AllowableValue o1, AllowableValue o2) {
                    if (o1 == null) {
                        return o2 == null ? 0 : 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            AllowableValue[] engines = engineList.toArray(new AllowableValue[engineList.size()]);

            SCRIPT_ENGINE = new PropertyDescriptor.Builder()
                    .name("Script Engine")
                    .required(true)
                    .description("The engine to execute scripts")
                    .allowableValues(engines)
                    .defaultValue(engines[0].getValue())
                    .required(true)
                    .expressionLanguageSupported(false)
                    .build();
            descriptors.add(SCRIPT_ENGINE);
        }

        descriptors.add(SCRIPT_FILE);
        descriptors.add(SCRIPT_BODY);
        descriptors.add(SCRIPT_ARGS);
        descriptors.add(MODULES);

        isInitialized.set(true);
    }

    /**
     * Returns the valid relationships for this processor.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_NOFLOWFILE);
        return Collections.unmodifiableSet(relationships);
    }

    /**
     * Returns a list of property descriptors supported by this processor. The list always includes properties such as
     * script engine name, script file name, script body name, script arguments, and an external module path. If the
     * scripted processor also defines supported properties, those are added to the list as well.
     *
     * @return a List of PropertyDescriptor objects supported by this processor
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                createResources();
            }
        }

        return Collections.unmodifiableList(descriptors);
    }

    /**
     * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
     * which will be available as variables in the script
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return a PropertyDescriptor object corresponding to the specified dynamic property name
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    /**
     * Determines whether the given path refers to a valid file
     *
     * @param path a path to a file
     * @return true if the path refers to a valid file, false otherwise
     */
    private boolean isFile(final String path) {
        return path != null && Files.isRegularFile(Paths.get(path));
    }

    /**
     * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
     * properties, as well as reloading the script (from file or the "Script Body" property)
     *
     * @param context the context in which to perform the setup operations
     */
    @OnScheduled
    public void setup(final ProcessContext context) {
        scriptEngineName = context.getProperty(SCRIPT_ENGINE).getValue();
        scriptPath = context.getProperty(SCRIPT_FILE).getValue();
        scriptBody = context.getProperty(SCRIPT_BODY).getValue();
        modulePath = context.getProperty(MODULES).getValue();
        setupEngine();
    }

    /**
     * Configures the specified script engine. First, the engine is loaded and instantiated using the JSR-223
     * javax.script APIs. Then, if any script configurators have been defined for this engine, their init() method is
     * called, and the configurator is saved for future calls.
     *
     * @see org.apache.nifi.processors.script.ScriptEngineConfigurator
     */
    private void setupEngine() {
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            ProcessorLog log = getLogger();

            // Need the right classloader when the engine is created. This ensures the NAR's execution class loader
            // (plus the module path) becomes the parent for the script engine
            ClassLoader scriptEngineModuleClassLoader = createScriptEngineModuleClassLoader(modulePath);
            if (scriptEngineModuleClassLoader != null) {
                Thread.currentThread().setContextClassLoader(scriptEngineModuleClassLoader);
            }
            scriptEngine = getScriptEngine();
            ServiceLoader<ScriptEngineConfigurator> configuratorServiceLoader =
                    ServiceLoader.load(ScriptEngineConfigurator.class);
            for (ScriptEngineConfigurator configurator : configuratorServiceLoader) {
                String configuratorScriptEngineName = configurator.getScriptEngineName();
                try {
                    if (configuratorScriptEngineName != null
                            && configuratorScriptEngineName.equals(scriptEngineName)) {
                        configurator.init(scriptEngine, modulePath);
                        scriptEngineConfiguratorMap.put(configurator.getScriptEngineName(), configurator);
                    }
                } catch (ScriptException se) {
                    log.error("Error initializing script engine configurator {}",
                            new Object[]{configuratorScriptEngineName});
                    if (log.isDebugEnabled()) {
                        log.error("Error initializing script engine configurator", se);
                    }
                }
            }
        } finally {
            // Restore original context class loader
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }


    /**
     * Provides a ScriptEngine corresponding to the currently selected script engine name.
     * ScriptEngineManager.getEngineByName() doesn't use find ScriptEngineFactory.getName(), which
     * is what we used to populate the list. So just search the list of factories until a match is
     * found, then create and return a script engine.
     *
     * @return a Script Engine corresponding to the currently specified name, or null if none is found.
     */
    private ScriptEngine getScriptEngine() {
        //
        ScriptEngineFactory factory = scriptEngineFactoryMap.get(scriptEngineName);
        if (factory == null) {
            return null;
        }
        return factory.getScriptEngine();
    }

    /**
     * Handles changes to this processor's properties. If changes are made to script- or engine-related properties,
     * the script will be reloaded.
     *
     * @param descriptor of the modified property
     * @param oldValue   non-null property value (previous)
     * @param newValue   the new property value or if null indicates the property
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        final ProcessorLog logger = getLogger();

        if (SCRIPT_FILE.equals(descriptor)
                || SCRIPT_BODY.equals(descriptor)
                || MODULES.equals(descriptor)
                || SCRIPT_ENGINE.equals(descriptor)) {
            lock.lock();
            try {
                // if the script is changing we'll want to reload the instance
                if (SCRIPT_FILE.equals(descriptor)) {
                    if (isFile(newValue)) {
                        scriptNeedsReload.set(true);

                        // we're attempted to load the script so we need to watch for updates
                        scriptWatcher = new SynchronousFileWatcher(Paths.get(newValue), new LastModifiedMonitor());
                    } else {
                        // the doesn't appear to be a file
                        scriptWatcher = null;
                    }

                    // always want to record the configured value
                    scriptPath = newValue;
                } else if (SCRIPT_BODY.equals(descriptor)) {

                    scriptNeedsReload.set(true);
                    scriptBody = newValue;

                } else if (MODULES.equals(descriptor)) {

                    // temporarily set new value (will be restored to oldValue if something goes wrong)
                    modulePath = newValue;
                    try {
                        setupEngine();

                        // we only want to reload during a module change if the script is already loaded
                        if (isFile(scriptPath)) {
                            scriptNeedsReload.set(true);
                        }
                    } catch (Throwable t) {
                        modulePath = oldValue;
                        logger.error(t.getLocalizedMessage(), t);
                    }

                } else if (SCRIPT_ENGINE.equals(descriptor)) {
                    // The script engine has changed, so we need to set up a new instance for the selected
                    // engine name
                    scriptEngineName = newValue;
                    setupEngine();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Creates a classloader to be used by the selected script engine and the provided script file. This
     * classloader has the ExecuteScript's classloader as a parent (versus the current thread's context
     * classloader) and also adds the specified module directory to the classpath. This enables scripts
     * to use other scripts, modules, etc. without having to build them into the ExecuteScript NAR.
     * If the parameter is null or empty, ExecuteScript's classloader is returned
     *
     * @param modulePath The path to a directory containing modules to be used by the script(s)
     */
    private ClassLoader createScriptEngineModuleClassLoader(String modulePath) {
        URLClassLoader newModuleClassLoader = null;
        if (StringUtils.isEmpty(modulePath)) {
            return ExecuteScript.class.getClassLoader();
        }
        try {
            newModuleClassLoader =
                    new URLClassLoader(
                            new URL[]{new File(modulePath).toURI().toURL()}, ExecuteScript.class.getClassLoader());
        } catch (MalformedURLException mue) {
            getLogger().error("Couldn't find modules directory at " + modulePath, mue);
        }
        return newModuleClassLoader;
    }

    /**
     * Evaluates the given script body (or file) using the current session, context, and flowfile. The script
     * evaluation expects a FlowFile to be returned, in which case it will route the FlowFile to success. If a script
     * error occurs, the original FlowFile will be routed to failure. If the script succeeds but does not return a
     * FlowFile, the original FlowFile will be routed to no-flowfile
     *
     * @param context the current process context
     * @param session the current process session
     * @throws ProcessException if any error occurs during script evaluation
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                createResources();
            }
        }
        ProcessorLog log = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String[] args = context.getProperty(SCRIPT_ARGS).evaluateAttributeExpressions().getValue().split(";");

        Bindings bindings = scriptEngine.createBindings();

        bindings.put("args", args);
        bindings.put(ScriptEngine.ARGV, Arrays.asList(args));
        bindings.put("session", session);
        bindings.put("flowFile", flowFile);
        bindings.put("log", log);

        // Find the user-added properties and set them on the script
        List<PropertyDescriptor> propertyDescriptors = getSupportedPropertyDescriptors();
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            if (!propertyDescriptors.contains(property.getKey())) {
                // The descriptor isn't one of the supported ones, so it must be dynamic. Add it to the bindings
                if (property.getValue() != null) {
                    bindings.put(property.getKey().getName(), property.getValue());
                }
            }
        }

        try {
            final StopWatch stopWatch = new StopWatch(true);
            Object scriptResult;
            try {
                // If the script has been compiled and nothing has changed, just execute it again
                if (!scriptNeedsReload.get() && compiledScript != null) {
                    scriptResult = compiledScript.eval(bindings);
                } else {
                    // Something has changed, let the configurator compile the script if need be, or if one doesn't
                    // exist, fall back to the engine itself
                    scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                    InputStream scriptStream;
                    if (scriptPath != null) {
                        scriptStream = new FileInputStream(scriptPath);
                    } else if (scriptBody != null) {
                        scriptStream = new ByteArrayInputStream(scriptBody.getBytes("UTF-8"));
                    } else {
                        throw new ProcessException("One of Script Path or Script Body must be set!");
                    }

                    // Create a BufferedReader in case we need it (only non-Compilable configurators don't)
                    Reader reader = new BufferedReader(new InputStreamReader(scriptStream));

                    // Execute any engine-specific configuration before the script is evaluated
                    ScriptEngineConfigurator configurator =
                            scriptEngineConfiguratorMap.get(scriptEngineName);

                    // Recompile (new or changed script) if available
                    if (scriptEngine instanceof Compilable) {
                        Compilable compiler;
                        if (configurator != null && configurator instanceof Compilable) {
                            compiler = ((Compilable) configurator);
                        } else {
                            compiler = ((Compilable) scriptEngine);
                        }
                        compiledScript = compiler.compile(reader);
                        scriptResult = compiledScript.eval(bindings);
                    } else {
                        // Evaluate the script with the configurator (if it exists) or the engine
                        if (configurator != null) {
                            scriptResult = configurator.eval(scriptEngine, scriptStream, modulePath);
                        } else {
                            scriptResult = scriptEngine.eval(reader);
                        }
                    }

                }

            } catch (IOException | ScriptException e) {
                throw new ProcessException(e);
            }

            if (scriptResult instanceof FlowFile) {
                flowFile = (FlowFile) scriptResult;
                log.info("Successfully processed flowFile {} ({}) in {} millis",
                        new Object[]{flowFile, flowFile.getSize(), stopWatch.getElapsed(TimeUnit.MILLISECONDS)});
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                if (scriptResult != null) {
                    log.info("Script result was not a FlowFile, instead it was "
                            + scriptResult.getClass().getCanonicalName());
                }
                session.transfer(flowFile, REL_NOFLOWFILE);
            }

        } catch (ProcessException pe) {
            log.error("Failed to process flowFile {} due to {}; routing to failure", new Object[]{flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
