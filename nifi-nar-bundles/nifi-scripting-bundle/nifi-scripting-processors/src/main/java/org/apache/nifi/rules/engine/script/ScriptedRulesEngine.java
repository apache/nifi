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
package org.apache.nifi.rules.engine.script;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.script.AbstractScriptedControllerService;
import org.apache.nifi.script.ScriptingComponentHelper;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"rules", "rules engine", "script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj"})
@CapabilityDescription("Allows the user to provide a scripted RulesEngineService for custom firing of rules depending on the supplied facts. The script must set a variable 'rulesEngine' to an "
        + "implementation of RulesEngineService.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
public class ScriptedRulesEngine extends AbstractScriptedControllerService implements RulesEngineService {

    protected final AtomicReference<RulesEngineService> rulesEngine = new AtomicReference<>();

    /**
     * Returns a list of property descriptors supported by this processor. The list always includes properties such as
     * script engine name, script file name, script body name, script arguments, and an external module path. If the
     * scripted processor also defines supported properties, those are added to the list as well.
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

        return Collections.unmodifiableList(scriptingComponentHelper.getDescriptors());
    }

    public void setup() {
        if (scriptNeedsReload.get() || rulesEngine.get() == null) {
            if (ScriptingComponentHelper.isFile(scriptingComponentHelper.getScriptPath())) {
                scriptNeedsReload.set(reloadScriptFile(scriptingComponentHelper.getScriptPath()));
            } else {
                scriptNeedsReload.set(reloadScriptBody(scriptingComponentHelper.getScriptBody()));
            }
        }
    }

    /**
     * Reloads the script RulesEngineService. This must be called within the lock.
     *
     * @param scriptBody An input stream associated with the script content
     * @return Whether the script was successfully reloaded
     */
    protected boolean reloadScript(final String scriptBody) {
        // note we are starting here with a fresh listing of validation
        // results since we are (re)loading a new/updated script. any
        // existing validation results are not relevant
        final Collection<ValidationResult> results = new HashSet<>();

        try {
            // Create a single script engine, the Processor object is reused by each task
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


                // get configured processor from the script (if it exists)
                final Object obj = scriptEngine.get("rulesEngine");
                if (obj != null) {
                    final ComponentLog logger = getLogger();

                    try {
                        // set the logger if the processor wants it
                        invocable.invokeMethod(obj, "setLogger", logger);
                    } catch (final NoSuchMethodException nsme) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Configured script RulesEngineService does not contain a setLogger method.");
                        }
                    }

                    if (configurationContext != null) {
                        try {
                            // set the logger if the processor wants it
                            invocable.invokeMethod(obj, "setConfigurationContext", configurationContext);
                        } catch (final NoSuchMethodException nsme) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Configured script RulesEngineService does not contain a setConfigurationContext method.");
                            }
                        }
                    }

                    // record the processor for use later
                    final RulesEngineService scriptedReader = invocable.getInterface(obj, RulesEngineService.class);
                    rulesEngine.set(scriptedReader);

                } else {
                    throw new ScriptException("No RecordReader was defined by the script.");
                }
            }

        } catch (final Exception ex) {
            final ComponentLog logger = getLogger();
            final String message = "Unable to load script: " + ex.getLocalizedMessage();

            logger.error(message, ex);
            results.add(new ValidationResult.Builder()
                    .subject("ScriptValidation")
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

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
            }
        }
        super.onEnabled(context);

        // Call an non-interface method onEnabled(context), to allow a scripted RulesEngineService the chance to set up as necessary
        if (scriptRunner != null) {
            final ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
            final Invocable invocable = (Invocable) scriptEngine;
            if (configurationContext != null) {
                try {
                    // Get the actual object from the script engine, versus the proxy stored in RulesEngineService. The object may have additional methods,
                    // where RulesEngineService is a proxied interface
                    final Object obj = scriptEngine.get("rulesEngine");
                    if (obj != null) {
                        try {
                            invocable.invokeMethod(obj, "onEnabled", context);
                        } catch (final NoSuchMethodException nsme) {
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug("Configured script RulesEngineService does not contain an onEnabled() method.");
                            }
                        }
                    } else {
                        throw new ScriptException("No RulesEngineService was defined by the script.");
                    }
                } catch (ScriptException se) {
                    throw new ProcessException("Error executing onEnabled(context) method: " + se.getMessage(), se);
                }
            }
        } else {
            throw new ProcessException("Error creating ScriptRunner");
        }
    }

    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        if (rulesEngine.get() != null) {
            return rulesEngine.get().fireRules(facts);
        }
        return null;
    }
}
