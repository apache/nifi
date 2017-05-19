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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import javax.script.ScriptEngine;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract class with common methods and variables for reuse among Controller Services
 */
public abstract class AbstractScriptedControllerService extends AbstractControllerService {

    protected final AtomicReference<Collection<ValidationResult>> validationResults = new AtomicReference<>(new ArrayList<>());

    protected final AtomicBoolean scriptNeedsReload = new AtomicBoolean(true);

    protected volatile ScriptEngine scriptEngine = null;
    protected volatile ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();
    protected volatile ConfigurationContext configurationContext = null;

    /**
     * Returns a list of property descriptors supported by this record reader. The
     * list always includes properties such as script engine name, script file
     * name, script body name, script arguments, and an external module path.
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

        if (ScriptingComponentUtils.SCRIPT_FILE.equals(descriptor)
                || ScriptingComponentUtils.SCRIPT_BODY.equals(descriptor)
                || ScriptingComponentUtils.MODULES.equals(descriptor)
                || scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {
            scriptNeedsReload.set(true);
            // Need to reset scriptEngine if the value has changed
            if (scriptingComponentHelper.SCRIPT_ENGINE.equals(descriptor)) {
                scriptEngine = null;
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return scriptingComponentHelper.customValidate(validationContext);
    }

    public void onEnabled(final ConfigurationContext context) {
        this.configurationContext = context;

        scriptingComponentHelper.setupVariables(context);
        setup();
    }

    abstract public void setup();

    /**
     * Reloads the script located at the given path
     *
     * @param scriptPath the path to the script file to be loaded
     * @return true if the script was loaded successfully; false otherwise
     */
    protected boolean reloadScriptFile(final String scriptPath) {
        final Collection<ValidationResult> results = new HashSet<>();

        try (final FileInputStream scriptStream = new FileInputStream(scriptPath)) {
            return reloadScript(IOUtils.toString(scriptStream, Charset.defaultCharset()));

        } catch (final Exception e) {
            final ComponentLog logger = getLogger();
            final String message = "Unable to load script: " + e;

            logger.error(message, e);
            results.add(new ValidationResult.Builder()
                    .subject("ScriptValidation")
                    .valid(false)
                    .explanation("Unable to load script due to " + e)
                    .input(scriptPath)
                    .build());
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
    protected boolean reloadScriptBody(final String scriptBody) {
        final Collection<ValidationResult> results = new HashSet<>();
        try {
            return reloadScript(scriptBody);

        } catch (final Exception e) {
            final ComponentLog logger = getLogger();
            final String message = "Unable to load script: " + e;

            logger.error(message, e);
            results.add(new ValidationResult.Builder()
                    .subject("ScriptValidation")
                    .valid(false)
                    .explanation("Unable to load script due to " + e)
                    .input(scriptingComponentHelper.getScriptPath())
                    .build());
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

    protected abstract boolean reloadScript(final String scriptBody);
}
