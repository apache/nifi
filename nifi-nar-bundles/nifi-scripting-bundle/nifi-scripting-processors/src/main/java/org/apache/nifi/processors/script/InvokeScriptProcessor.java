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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "scala"})
@CapabilityDescription("Invokes a script engine for a Processor defined in the given script")
@DynamicProperty(name = "A script engine property to update", value = "The value to set it to", supportsExpressionLanguage = true,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@SeeAlso({ExecuteScript.class})
public class InvokeScriptProcessor extends AbstractSessionFactoryProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that were failed to process")
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
            .description("Path to a directory which contains modules required by the script.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(new StandardValidators.DirectoryExistsValidator(true, false))
            .build();

    // A map from engine name to a custom configurator for that engine
    private static final Map<String, ScriptEngineConfigurator>
            scriptEngineConfiguratorMap = new ConcurrentHashMap<>();

    private final AtomicReference<Processor> processor = new AtomicReference<>();
    private final AtomicReference<Collection<ValidationResult>> validationResults =
            new AtomicReference(Collections.EMPTY_LIST);

    private ControllerServiceLookup controllerServiceLookup;
    private String initializationContextId;

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private SynchronousFileWatcher scriptWatcher;

    private Map<String, ScriptEngineFactory> scriptEngineFactoryMap;
    private ScriptEngineManager scriptEngineManager;
    private ScriptEngine scriptEngine;
    private String scriptEngineName;
    private String scriptPath;
    private String modulePath;
    private String scriptBody;

    private ScheduledExecutorService reloadService = null;

    private List<PropertyDescriptor> descriptors;

    /**
     * Initializes this processor
     *
     * @param context in which to perform initialization
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {
        initializationContextId = context.getIdentifier();
        controllerServiceLookup = context.getControllerServiceLookup();
    }


    /**
     * Creates the resources needed by this processor. An attempt is made to also initialize the scripted processor,
     * but unless the properties (such as script engine name and script file path) have already been specified, the
     * script will not yet have been evaluated, so the script's initialize() method will not be called.
     */
    protected void createResources() {

        // Set up script file reloader service. This checks to see if the script file has changed, and if so, tries
        // to reload it
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
                                        // reload the actual script
                                        final boolean reloaded = reloadScriptFile(scriptPath);

                                        // log the script was reloaded
                                        if (reloaded) {
                                            getLogger().info("The configured script has been successfully reloaded.");
                                        }
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

        // The following is required for JRuby, should be transparent to everything else
        System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");

        // Create list of available engines
        scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        if (scriptEngineFactories != null) {
            scriptEngineFactoryMap = new HashMap<>();
            List<AllowableValue> engineList = new ArrayList<>();
            for (ScriptEngineFactory factory : scriptEngineFactories) {
                ScriptEngine engine = factory.getScriptEngine();
                if (engine instanceof Invocable) {
                    engineList.add(new AllowableValue(factory.getLanguageName()));
                    scriptEngineFactoryMap.put(factory.getLanguageName(), factory);
                }
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
                    .description("The engine to execute the scripted Processor")
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
     * Returns the valid relationships for this processor. SUCCESS and FAILURE are always returned, and if the script
     * processor has defined additional relationships, those will be added as well.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        final Processor instance = processor.get();
        if (instance != null) {
            try {
                relationships.addAll(instance.getRelationships());
            } catch (final Throwable t) {
                final ProcessorLog logger = getLogger();
                final String message = "Unable to get relationships from scripted Processor: " + t;

                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }
            }
        } else {
            // Return defaults for now
            relationships.add(REL_SUCCESS);
            relationships.add(REL_FAILURE);
        }
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
        List<PropertyDescriptor> supportedPropertyDescriptors = new ArrayList<>();
        supportedPropertyDescriptors.addAll(descriptors);

        final Processor instance = processor.get();
        if (instance != null) {
            try {
                final List<PropertyDescriptor> instanceDescriptors = instance.getPropertyDescriptors();
                if (instanceDescriptors != null) {
                    supportedPropertyDescriptors.addAll(instanceDescriptors);
                }
            } catch (final Throwable t) {
                final ProcessorLog logger = getLogger();
                final String message = "Unable to get property descriptors from Processor: " + t;

                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }
            }
        }

        return Collections.unmodifiableList(supportedPropertyDescriptors);
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
        if (processor.get() == null) {
            if (isFile(scriptPath)) {
                reloadScriptFile(scriptPath);
            } else {
                reloadScriptBody(scriptBody);
            }
        }
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
                            && configuratorScriptEngineName.equalsIgnoreCase(scriptEngineName)) {
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
        final Processor instance = processor.get();

        if (SCRIPT_FILE.equals(descriptor)
                || SCRIPT_BODY.equals(descriptor)
                || MODULES.equals(descriptor)
                || SCRIPT_ENGINE.equals(descriptor)) {
            lock.lock();
            try {
                // if the script is changing we'll want to reload the instance
                if (SCRIPT_FILE.equals(descriptor)) {
                    if (isFile(newValue)) {
                        reloadScriptFile(newValue);

                        // we're attempted to load the script so we need to watch for updates
                        scriptWatcher = new SynchronousFileWatcher(Paths.get(newValue), new LastModifiedMonitor());
                    } else {
                        // the doesn't appear to be a file
                        scriptWatcher = null;
                    }

                    // always want to record the configured value
                    scriptPath = newValue;
                } else if (SCRIPT_BODY.equals(descriptor)) {

                    if (reloadScriptBody(newValue)) {
                        // always want to record the configured value
                        scriptBody = newValue;
                    }
                } else if (MODULES.equals(descriptor)) {

                    // temporarily set new value (will be restored to oldValue if something goes wrong)
                    modulePath = newValue;
                    try {
                        setupEngine();

                        boolean reloaded = false;

                        // we only want to reload during a module change if the script is already loaded
                        if (scriptPath != null || scriptBody != null) {
                            if (isFile(scriptPath)) {
                                // reload the script
                                reloaded = reloadScriptFile(scriptPath);
                            } else if (scriptBody != null) {
                                reloaded = reloadScriptBody(scriptBody);
                            }
                            // log the script was reloaded
                            if (reloaded) {
                                logger.info("The configured script has been successfully reloaded.");
                            } else {
                                throw new ProcessException("The configured script could not be reloaded");
                            }
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
        } else if (instance != null) {
            // If the script provides a Processor, call its onPropertyModified() method
            try {
                instance.onPropertyModified(descriptor, oldValue, newValue);
            } catch (final Throwable t) {
                final String message = "Unable to invoke onPropertyModified from script Processor: " + t;

                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }
            }
        }
    }

    /**
     * Creates a classloader to be used by the selected script engine and the provided script file. This
     * classloader has the InvokeScriptProcessor's classloader as a parent (versus the current thread's context
     * classloader) and also adds the specified module directory to the classpath. This enables scripts
     * to use other scripts, modules, etc. without having to build them into the InvokeScriptProcessor NAR.
     * If the parameter is null or empty, InvokeScriptProcessor's classloader is returned
     *
     * @param modulePath The path to a directory containing modules to be used by the script(s)
     */
    private ClassLoader createScriptEngineModuleClassLoader(String modulePath) {
        URLClassLoader newModuleClassLoader = null;
        if (StringUtils.isEmpty(modulePath)) {
            return InvokeScriptProcessor.class.getClassLoader();
        }
        try {
            newModuleClassLoader =
                    new URLClassLoader(
                            new URL[]{new File(modulePath).toURI().toURL()},
                            InvokeScriptProcessor.class.getClassLoader());
        } catch (MalformedURLException mue) {
            getLogger().error("Couldn't find modules directory at " + modulePath, mue);
        }
        return newModuleClassLoader;
    }


    /**
     * Reloads the script located at the given path
     *
     * @param scriptPath the path to the script file to be loaded
     * @return true if the script was loaded successfully; false otherwise
     */
    private boolean reloadScriptFile(final String scriptPath) {
        final Collection<ValidationResult> results = new HashSet<>();

        try (final FileInputStream scriptStream = new FileInputStream(scriptPath)) {
            return reloadScriptFromReader(scriptStream);

        } catch (final Throwable t) {
            final ProcessorLog logger = getLogger();
            final String message = "Unable to load script: " + t;

            // If the module path has not yet been set, then this script is likely being loaded too early and depends
            // on modules the processor does not yet know about. If this is the case, it will be reloaded later on
            // property change (modules) or when scheduled
            if (modulePath != null) {
                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }

                results.add(new ValidationResult.Builder()
                        .subject("ScriptValidation")
                        .valid(false)
                        .explanation("Unable to load script due to " + t)
                        .input(scriptPath)
                        .build());
            }
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

    /**
     * Reloads the script defined by the given string
     *
     * @param scriptBody the contents of the script to be loaded
     * @return true if the script was loaded successfully; false otherwise
     */
    private boolean reloadScriptBody(final String scriptBody) {
        final Collection<ValidationResult> results = new HashSet<>();
        try (final ByteArrayInputStream scriptStream = new ByteArrayInputStream(scriptBody.getBytes("UTF-8"))) {
            return reloadScriptFromReader(scriptStream);

        } catch (final Throwable t) {
            final ProcessorLog logger = getLogger();
            final String message = "Unable to load script: " + t;

            // If the module path has not yet been set, then this script is likely being loaded too early and depends
            // on modules the processor does not yet know about. If this is the case, it will be reloaded later on
            // property change (modules) or when scheduled
            if (modulePath != null) {
                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }

                results.add(new ValidationResult.Builder()
                        .subject("ScriptValidation")
                        .valid(false)
                        .explanation("Unable to load script due to " + t)
                        .input(scriptPath)
                        .build());
            }
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

    /**
     * Reloads the script Processor. This must be called within the lock.
     *
     * @param scriptStream An input stream associated with the script content
     * @returns Whether the script was successfully reloaded
     */
    private boolean reloadScriptFromReader(final InputStream scriptStream) {
        // note we are starting here with a fresh listing of validation
        // results since we are (re)loading a new/updated script. any
        // existing validation results are not relevant
        final Collection<ValidationResult> results = new HashSet<>();

        try {
            // get the engine and ensure its invocable
            if (scriptEngine instanceof Invocable) {
                final Invocable invocable = (Invocable) scriptEngine;

                // Find a custom configurator and invoke their eval() method
                ScriptEngineConfigurator configurator =
                        scriptEngineConfiguratorMap.get(scriptEngineName);
                if (configurator != null) {
                    configurator.eval(scriptEngine, scriptStream, modulePath);
                } else {
                    // evaluate the script
                    scriptEngine.eval(new BufferedReader(new InputStreamReader(scriptStream)));
                }

                // get configured processor from the script (if it exists)
                final Object obj = scriptEngine.get("processor");
                final ProcessorLog logger = getLogger();

                try {
                    // set the logger if the processor wants it
                    invocable.invokeMethod(obj, "setLogger", logger);
                } catch (final NoSuchMethodException nsme) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Configured script Processor does not contain a setLogger method.");
                    }
                }

                // record the processor for use later
                final Processor scriptProcessor = invocable.getInterface(obj, Processor.class);
                processor.set(scriptProcessor);

                if (scriptProcessor != null) {
                    try {
                        scriptProcessor.initialize(new ProcessorInitializationContext() {
                            @Override
                            public String getIdentifier() {
                                return initializationContextId;
                            }

                            @Override
                            public ProcessorLog getLogger() {
                                return logger;
                            }

                            @Override
                            public ControllerServiceLookup getControllerServiceLookup() {
                                return controllerServiceLookup;
                            }
                        });
                    } catch (final Throwable t) {
                        final String message = "Unable to initialize scripted Processor: " + t;

                        logger.error(message);
                        if (logger.isDebugEnabled()) {
                            logger.error(message, t);
                        }
                    }
                }
            }

        } catch (final Throwable t) {
            final ProcessorLog logger = getLogger();
            final String message = "Unable to load script: " + t;

            // If the module path has not yet been set, then this script is likely being loaded too early and depends
            // on modules the processor does not yet know about. If this is the case, it will be reloaded later on
            // property change (modules) or when scheduled
            //
            // Alternatively, if the module is
            if (modulePath != null) {
                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }

                results.add(new ValidationResult.Builder()
                        .subject("ScriptValidation")
                        .valid(false)
                        .explanation("Unable to load script due to " + t)
                        .input(scriptPath)
                        .build());
            }
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

    /**
     * Invokes the validate() routine provided by the script, allowing for custom validation code.
     * This method assumes there is a valid Processor defined in the script and it has been loaded
     * by the InvokeScriptProcessor processor
     *
     * @param context The validation context to be passed into the custom validate method
     * @return A collection of ValidationResults returned by the custom validate method
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Processor instance = processor.get();
        final Collection<ValidationResult> currentValidationResults = validationResults.get();

        // if there was existing validation errors and the processor loaded successfully
        if (currentValidationResults.isEmpty() && instance != null) {
            try {
                // defer to the underlying processor for validation
                final Collection<ValidationResult> instanceResults = instance.validate(context);
                if (instanceResults != null && instanceResults.size() > 0) {
                    // return the validation results from the underlying instance
                    return instanceResults;
                }
            } catch (final Throwable t) {
                final ProcessorLog logger = getLogger();
                final String message = "Unable to validate the script Processor: " + t;

                logger.error(message);
                if (logger.isDebugEnabled()) {
                    logger.error(message, t);
                }

                // return a new validation message
                final Collection<ValidationResult> results = new HashSet<>();
                results.add(new ValidationResult.Builder()
                        .subject("Validation")
                        .valid(false)
                        .explanation("An error occurred calling validate in the configured script Processor.")
                        .input(context.getProperty(SCRIPT_FILE).getValue())
                        .build());
                return results;
            }
        }

        return currentValidationResults;
    }

    /**
     * Invokes the onTrigger() method of the scripted processor. If the script failed to reload, the processor yields
     * until the script can be reloaded successfully. If the scripted processor's onTrigger() method throws an
     * exception, a ProcessException will be thrown. If no processor is defined by the script, an error is logged
     * with the system.
     *
     * @param context        provides access to convenience methods for obtaining
     *                       property values, delaying the scheduling of the processor, provides
     *                       access to Controller Services, etc.
     * @param sessionFactory provides access to a {@link ProcessSessionFactory}, which
     *                       can be used for accessing FlowFiles, etc.
     * @throws ProcessException if the scripted processor's onTrigger() method throws an exception
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

        // Initialize the rest of the processor resources if we have not already done so
        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                createResources();
            }
        }

        ProcessorLog log = getLogger();

        // ensure the processor (if it exists) is loaded
        final Processor instance = processor.get();
        if (instance != null) {

            // ensure the processor did not fail to reload at some point
            final Collection<ValidationResult> results = validationResults.get();
            if (!results.isEmpty()) {
                log.error(String.format("Unable to run because the Processor is not valid: [%s]",
                        StringUtils.join(results, ", ")));
                context.yield();
                return;
            }

            try {
                // run the processor
                instance.onTrigger(context, sessionFactory);
            } catch (final Throwable t) {
                final String message = String.format("An error occurred executing the configured Processor [%s]: %s", context.getProperty(SCRIPT_FILE).getValue(), t);

                log.error(message);
                if (log.isDebugEnabled()) {
                    log.error(message, t);
                }

                throw t;
            }
        } else {
            log.error("There is no processor defined by the script");
        }
    }
}
