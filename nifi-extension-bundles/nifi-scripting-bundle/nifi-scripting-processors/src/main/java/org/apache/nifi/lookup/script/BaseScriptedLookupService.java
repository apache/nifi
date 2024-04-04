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

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.script.AbstractScriptedControllerService;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.script.impl.FilteredPropertiesValidationContextAdapter;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BaseScriptedLookupService extends AbstractScriptedControllerService {

    protected final AtomicReference<LookupService> lookupService = new AtomicReference<>();

    protected volatile String kerberosServicePrincipal = null;
    protected volatile File kerberosConfigFile = null;
    protected volatile File kerberosServiceKeytab = null;

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
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.addAll(scriptingComponentHelper.getDescriptors());
        _temp.remove(scriptingComponentHelper.SCRIPT_ENGINE);

        PropertyDescriptor.Builder engineProp = new PropertyDescriptor
                .Builder().fromPropertyDescriptor(scriptingComponentHelper.SCRIPT_ENGINE);
        List<AllowableValue> filtered = scriptingComponentHelper.getScriptEngineAllowableValues()
                .stream()
                .collect(Collectors.toList());
        engineProp.allowableValues(filtered.toArray(new AllowableValue[filtered.size()]));

        supportedPropertyDescriptors.add(engineProp.build());
        supportedPropertyDescriptors.addAll(_temp);

        final ConfigurableComponent instance = lookupService.get();
        if (instance != null) {
            try {
                final List<PropertyDescriptor> instanceDescriptors = instance.getPropertyDescriptors();
                if (instanceDescriptors != null) {
                    supportedPropertyDescriptors.addAll(instanceDescriptors);
                }
            } catch (final Throwable t) {
                final ComponentLog logger = getLogger();
                final String message = "Unable to get property descriptors from LookupService: " + t;

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
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
        validationResults.set(new HashSet<>());

        final ComponentLog logger = getLogger();
        final LookupService<?> instance = lookupService.get();

        if (ScriptingComponentUtils.SCRIPT_FILE.equals(descriptor)
                || ScriptingComponentUtils.SCRIPT_BODY.equals(descriptor)
                || ScriptingComponentUtils.MODULES.equals(descriptor)
                || scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {

            // Update the ScriptingComponentHelper's value(s)
            if (ScriptingComponentUtils.SCRIPT_FILE.equals(descriptor)) {
                scriptingComponentHelper.setScriptPath(newValue);
            } else if (ScriptingComponentUtils.SCRIPT_BODY.equals(descriptor)) {
                scriptingComponentHelper.setScriptBody(newValue);
            } else if (scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {
                scriptingComponentHelper.setScriptEngineName(newValue);
            }

            scriptNeedsReload.set(true);
            scriptRunner = null; //reset engine. This happens only when the controller service is disabled, so there won't be any performance impact in run-time.
        } else if (instance != null) {
            // If the script provides a LookupService, call its onPropertyModified() method
            try {
                instance.onPropertyModified(descriptor, oldValue, newValue);
            } catch (final Exception e) {
                final String message = "Unable to invoke onPropertyModified from scripted LookupService: " + e;
                logger.error(message, e);
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        Collection<ValidationResult> commonValidationResults = super.customValidate(context);
        if (!commonValidationResults.isEmpty()) {
            return commonValidationResults;
        }

        // do not try to build processor/compile/etc until onPropertyModified clear the validation error/s
        // and don't print anything into log.
        if (!validationResults.get().isEmpty()) {
            return validationResults.get();
        }

        Collection<ValidationResult> scriptingComponentHelperResults = scriptingComponentHelper.customValidate(context);
        if (scriptingComponentHelperResults != null && !scriptingComponentHelperResults.isEmpty()) {
            validationResults.set(scriptingComponentHelperResults);
            return scriptingComponentHelperResults;
        }

        scriptingComponentHelper.setScriptEngineName(context.getProperty(scriptingComponentHelper.SCRIPT_ENGINE).getValue());
        scriptingComponentHelper.setScriptPath(context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).evaluateAttributeExpressions().getValue());
        scriptingComponentHelper.setScriptBody(context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue());
        final ResourceReferences resourceReferences = context.getProperty(ScriptingComponentUtils.MODULES).evaluateAttributeExpressions().asResources();
        scriptingComponentHelper.setModules(resourceReferences);
        setup();

        // Now that the component is validated, we can call validate on the scripted lookup service
        final LookupService<?> instance = lookupService.get();
        final Collection<ValidationResult> currentValidationResults = validationResults.get();

        // if there was existing validation errors and the processor loaded successfully
        if (currentValidationResults.isEmpty() && instance != null) {
            try {
                // defer to the underlying controller service for validation, without the
                // lookup service's properties
                final Set<PropertyDescriptor> innerPropertyDescriptor = new HashSet<>(scriptingComponentHelper.getDescriptors());

                ValidationContext innerValidationContext = new FilteredPropertiesValidationContextAdapter(context, innerPropertyDescriptor);
                final Collection<ValidationResult> instanceResults = instance.validate(innerValidationContext);

                if (instanceResults != null && instanceResults.size() > 0) {
                    // return the validation results from the underlying instance
                    return instanceResults;
                }
            } catch (final Exception e) {
                final ComponentLog logger = getLogger();
                final String message = "Unable to validate the scripted LookupService: " + e;
                logger.error(message, e);

                // return a new validation message
                final Collection<ValidationResult> results = new HashSet<>();
                results.add(new ValidationResult.Builder()
                        .subject("Validation")
                        .valid(false)
                        .explanation("An error occurred calling validate in the configured scripted LookupService.")
                        .input(context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).getValue())
                        .build());
                return results;
            }
        }

        return currentValidationResults;

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
        if (scriptRunner != null) {
            final ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
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
        } else {
            throw new ProcessException("Error creating ScriptRunner");
        }
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        // Call an non-interface method onDisabled(context), to allow a scripted LookupService the chance to shut down as necessary
        if (scriptRunner != null) {
            final ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
            final Invocable invocable = (Invocable) scriptEngine;
            if (configurationContext != null) {
                try {
                    // Get the actual object from the script engine, versus the proxy stored in lookupService. The object may have additional methods,
                    // where lookupService is a proxied interface
                    final Object obj = scriptRunner.getScriptEngine().get("lookupService");
                    if (obj != null) {
                        try {
                            invocable.invokeMethod(obj, "onDisabled", context);
                        } catch (final NoSuchMethodException nsme) {
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug("Configured script LookupService does not contain an onDisabled() method.");
                            }
                        }
                    } else {
                        // This might be due to an error during compilation, log it rather than throwing an exception
                        getLogger().warn("No LookupService was defined by the script.");
                    }
                } catch (ScriptException se) {
                    throw new ProcessException("Error executing onDisabled(context) method", se);
                }
            }
        } else {
            throw new ProcessException("Error creating ScriptRunner");
        }

        scriptingComponentHelper.stop();
        lookupService.set(null);
        scriptRunner = null;
    }

    @Override
    public void setup() {
        if (scriptNeedsReload.get() || lookupService.get() == null) {
            if (ScriptingComponentHelper.isFile(scriptingComponentHelper.getScriptPath())) {
                scriptNeedsReload.set(!reloadScriptFile(scriptingComponentHelper.getScriptPath()));
            } else {
                scriptNeedsReload.set(!reloadScriptBody(scriptingComponentHelper.getScriptBody()));
            }
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
            // Create a single script engine, the LookupService object is reused by each task
            if (scriptRunner == null) {
                scriptingComponentHelper.setupScriptRunners(1, scriptBody, getLogger());
                scriptRunner = scriptingComponentHelper.scriptRunnerQ.poll();
            }

            if (scriptRunner == null) {
                throw new ProcessException("No script runner available!");
            }

            // get the engine and ensure its invocable
            ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
            if (scriptEngine instanceof Invocable) {
                final Invocable invocable = (Invocable) scriptEngine;

                // evaluate the script
                scriptRunner.run(scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE));

                // get configured LookupService from the script (if it exists)
                final Object obj = scriptRunner.getScriptEngine().get("lookupService");
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
                                    return BaseScriptedLookupService.this.getIdentifier();
                                }

                                @Override
                                public ComponentLog getLogger() {
                                    return logger;
                                }

                                @Override
                                public StateManager getStateManager() {
                                    return BaseScriptedLookupService.this.getStateManager();
                                }

                                @Override
                                public NodeTypeProvider getNodeTypeProvider() {
                                    return BaseScriptedLookupService.this.getNodeTypeProvider();
                                }

                                @Override
                                public ControllerServiceLookup getControllerServiceLookup() {
                                    return BaseScriptedLookupService.super.getControllerServiceLookup();
                                }

                                @Override
                                public String getKerberosServicePrincipal() {
                                    return BaseScriptedLookupService.this.kerberosServicePrincipal;
                                }

                                @Override
                                public File getKerberosServiceKeytab() {
                                    return BaseScriptedLookupService.this.kerberosServiceKeytab;
                                }

                                @Override
                                public File getKerberosConfigurationFile() {
                                    return BaseScriptedLookupService.this.kerberosConfigFile;
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
