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
package org.apache.nifi.lookup.script;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.script.ScriptEngineConfigurator;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.script.AbstractScriptedControllerService;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Controller service that allows the user to script the lookup operation to be performed (by LookupRecord, e.g.)
 */
@Tags({"lookup", "record", "script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "restricted"})
@CapabilityDescription("Allows the user to provide a scripted LookupService instance in order to enrich records from an incoming flow file.")
@Restricted("Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
public class ScriptedLookupService extends AbstractScriptedControllerService implements LookupService<Object> {

    protected final AtomicReference<LookupService<Object>> lookupService = new AtomicReference<>();

    private volatile String kerberosServicePrincipal = null;
    private volatile File kerberosConfigFile = null;
    private volatile File kerberosServiceKeytab = null;

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        // Delegate the lookup() call to the scripted LookupService
        return lookupService.get().lookup(coordinates);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return lookupService.get().getRequiredKeys();
    }

    @Override
    public Class<?> getValueType() {
        // Delegate the getValueType() call to the scripted LookupService
        return lookupService.get().getValueType();
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        kerberosServicePrincipal = context.getKerberosServicePrincipal();
        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosServiceKeytab = context.getKerberosServiceKeytab();
    }

    /**
     * Returns a list of property descriptors supported by this processor. The
     * list always includes properties such as script engine name, script file
     * name, script body name, script arguments, and an external module path. If
     * the scripted processor also defines supported properties, those are added
     * to the list as well.
     *
     * @return a List of PropertyDescriptor objects supported by this processor
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
            }
        }
        List<PropertyDescriptor> supportedPropertyDescriptors = new ArrayList<>();
        supportedPropertyDescriptors.addAll(scriptingComponentHelper.getDescriptors());

        final ConfigurableComponent instance = lookupService.get();
        if (instance != null) {
            try {
                final List<PropertyDescriptor> instanceDescriptors = instance.getPropertyDescriptors();
                if (instanceDescriptors != null) {
                    supportedPropertyDescriptors.addAll(instanceDescriptors);
                }
            } catch (final Throwable t) {
                final ComponentLog logger = getLogger();
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
     * Returns a PropertyDescriptor for the given name. This is for the user to
     * be able to define their own properties which will be available as
     * variables in the script
     *
     * @param propertyDescriptorName used to lookup if any property descriptors
     *                               exist for that name
     * @return a PropertyDescriptor object corresponding to the specified
     * dynamic property name
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    /**
     * Handles changes to this processor's properties. If changes are made to
     * script- or engine-related properties, the script will be reloaded.
     *
     * @param descriptor of the modified property
     * @param oldValue   non-null property value (previous)
     * @param newValue   the new property value or if null indicates the property
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        final ComponentLog logger = getLogger();
        final ConfigurableComponent instance = lookupService.get();

        if (ScriptingComponentUtils.SCRIPT_FILE.equals(descriptor)
                || ScriptingComponentUtils.SCRIPT_BODY.equals(descriptor)
                || ScriptingComponentUtils.MODULES.equals(descriptor)
                || scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {
            scriptNeedsReload.set(true);
            // Need to reset scriptEngine if the value has changed
            if (scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {
                scriptEngine = null;
            }
        } else if (instance != null) {
            // If the script provides a ConfigurableComponent, call its onPropertyModified() method
            try {
                instance.onPropertyModified(descriptor, oldValue, newValue);
            } catch (final Exception e) {
                final String message = "Unable to invoke onPropertyModified from scripted LookupService: " + e;
                logger.error(message, e);
            }
        }
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
            }
        }
        super.onEnabled(context);

        // Call an non-interface method onEnabled(context), to allow a scripted LookupService the chance to set up as necessary
        final Invocable invocable = (Invocable) scriptEngine;
        if (configurationContext != null) {
            try {
                // Get the actual object from the script engine, versus the proxy stored in lookupService. The object may have additional methods,
                // where lookupService is a proxied interface
                final Object obj = scriptEngine.get("lookupService");
                if (obj != null) {
                    try {
                        invocable.invokeMethod(obj, "onEnabled", context);
                    } catch (final NoSuchMethodException nsme) {
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("Configured script LookupService does not contain an onEnabled() method.");
                        }
                    }
                } else {
                    throw new ScriptException("No LookupService was defined by the script.");
                }
            } catch (ScriptException se) {
                throw new ProcessException("Error executing onEnabled(context) method", se);
            }
        }
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        // Call an non-interface method onDisabled(context), to allow a scripted LookupService the chance to shut down as necessary
        final Invocable invocable = (Invocable) scriptEngine;
        if (configurationContext != null) {
            try {
                // Get the actual object from the script engine, versus the proxy stored in lookupService. The object may have additional methods,
                // where lookupService is a proxied interface
                final Object obj = scriptEngine.get("lookupService");
                if (obj != null) {
                    try {
                        invocable.invokeMethod(obj, "onDisabled", context);
                    } catch (final NoSuchMethodException nsme) {
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("Configured script LookupService does not contain an onDisabled() method.");
                        }
                    }
                } else {
                    throw new ScriptException("No LookupService was defined by the script.");
                }
            } catch (ScriptException se) {
                throw new ProcessException("Error executing onDisabled(context) method", se);
            }
        }
    }

    @Override
    public void setup() {
        // Create a single script engine, the Processor object is reused by each task
        if (scriptEngine == null) {
            scriptingComponentHelper.setup(1, getLogger());
            scriptEngine = scriptingComponentHelper.engineQ.poll();
        }

        if (scriptEngine == null) {
            throw new ProcessException("No script engine available!");
        }

        if (scriptNeedsReload.get() || lookupService.get() == null) {
            if (ScriptingComponentHelper.isFile(scriptingComponentHelper.getScriptPath())) {
                reloadScriptFile(scriptingComponentHelper.getScriptPath());
            } else {
                reloadScriptBody(scriptingComponentHelper.getScriptBody());
            }
            scriptNeedsReload.set(false);
        }
    }

    /**
     * Reloads the script RecordReaderFactory. This must be called within the lock.
     *
     * @param scriptBody An input stream associated with the script content
     * @return Whether the script was successfully reloaded
     */
    @Override
    protected boolean reloadScript(final String scriptBody) {
        // note we are starting here with a fresh listing of validation
        // results since we are (re)loading a new/updated script. any
        // existing validation results are not relevant
        final Collection<ValidationResult> results = new HashSet<>();

        try {
            // get the engine and ensure its invocable
            if (scriptEngine instanceof Invocable) {
                final Invocable invocable = (Invocable) scriptEngine;

                // Find a custom configurator and invoke their eval() method
                ScriptEngineConfigurator configurator = scriptingComponentHelper.scriptEngineConfiguratorMap.get(scriptingComponentHelper.getScriptEngineName().toLowerCase());
                if (configurator != null) {
                    configurator.eval(scriptEngine, scriptBody, scriptingComponentHelper.getModules());
                } else {
                    // evaluate the script
                    scriptEngine.eval(scriptBody);
                }

                // get configured LookupService from the script (if it exists)
                final Object obj = scriptEngine.get("lookupService");
                if (obj != null) {
                    final ComponentLog logger = getLogger();

                    try {
                        // set the logger if the processor wants it
                        invocable.invokeMethod(obj, "setLogger", logger);
                    } catch (final NoSuchMethodException nsme) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Scripted LookupService does not contain a setLogger method.");
                        }
                    }

                    // record the processor for use later
                    final LookupService<Object> scriptedLookupService = invocable.getInterface(obj, LookupService.class);
                    lookupService.set(scriptedLookupService);

                    if (scriptedLookupService != null) {
                        try {
                            scriptedLookupService.initialize(new ControllerServiceInitializationContext() {
                                @Override
                                public String getIdentifier() {
                                    return ScriptedLookupService.this.getIdentifier();
                                }

                                @Override
                                public ComponentLog getLogger() {
                                    return logger;
                                }

                                @Override
                                public StateManager getStateManager() {
                                    return ScriptedLookupService.this.getStateManager();
                                }

                                @Override
                                public ControllerServiceLookup getControllerServiceLookup() {
                                    return ScriptedLookupService.super.getControllerServiceLookup();
                                }

                                @Override
                                public String getKerberosServicePrincipal() {
                                    return ScriptedLookupService.this.kerberosServicePrincipal;
                                }

                                @Override
                                public File getKerberosServiceKeytab() {
                                    return ScriptedLookupService.this.kerberosServiceKeytab;
                                }

                                @Override
                                public File getKerberosConfigurationFile() {
                                    return ScriptedLookupService.this.kerberosConfigFile;
                                }
                            });
                        } catch (final Exception e) {
                            logger.error("Unable to initialize scripted LookupService: " + e.getLocalizedMessage(), e);
                            throw new ProcessException(e);
                        }
                    }

                } else {
                    throw new ScriptException("No LookupService was defined by the script.");
                }
            } else {
                throw new ScriptException("Script engine is not Invocable, cannot be used for ScriptedLookupService");
            }

        } catch (final Exception ex) {
            final ComponentLog logger = getLogger();
            final String message = "Unable to load script: " + ex.getLocalizedMessage();

            logger.error(message, ex);
            results.add(new ValidationResult.Builder()
                    .subject("ScriptedLookupServiceValidation")
                    .valid(false)
                    .explanation("Unable to load script due to " + ex.getLocalizedMessage())
                    .input(scriptingComponentHelper.getScriptPath())
                    .build());
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

}
