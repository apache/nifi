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


import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"script", "execute", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "clojure"})
@CapabilityDescription("Experimental - Executes a script given the flow file and a process session.  The script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the script. If the handling is incomplete or incorrect, the session will be rolled back. Experimental: "
        + "Impact of sustained usage not yet verified.")
@DynamicProperty(
        name = "A script engine property to update",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value "
                + "specified by the Dynamic Property's value")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
@InputRequirement(Requirement.INPUT_ALLOWED)
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER},
        description = "Scripts can store and retrieve state using the State Management APIs. Consult the State Manager section of the Developer's Guide for more details.")
@SeeAlso({InvokeScriptedProcessor.class})
public class ExecuteScript extends AbstractSessionFactoryProcessor {

    // Constants maintained for backwards compatibility
    public static final Relationship REL_SUCCESS = ScriptingComponentUtils.REL_SUCCESS;
    public static final Relationship REL_FAILURE = ScriptingComponentUtils.REL_FAILURE;

    private String scriptToRun = null;
    volatile ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();


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
        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
            }
        }

        return Collections.unmodifiableList(scriptingComponentHelper.getDescriptors());
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
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return scriptingComponentHelper.customValidate(validationContext);
    }

    /**
     * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
     * properties, as well as reloading the script (from file or the "Script Body" property)
     *
     * @param context the context in which to perform the setup operations
     */
    @OnScheduled
    public void setup(final ProcessContext context) {
        scriptingComponentHelper.setupVariables(context);

        // Create a script engine for each possible task
        int maxTasks = context.getMaxConcurrentTasks();
        scriptingComponentHelper.setup(maxTasks, getLogger());
        scriptToRun = scriptingComponentHelper.getScriptBody();

        try {
            if (scriptToRun == null && scriptingComponentHelper.getScriptPath() != null) {
                try (final FileInputStream scriptStream = new FileInputStream(scriptingComponentHelper.getScriptPath())) {
                    scriptToRun = IOUtils.toString(scriptStream, Charset.defaultCharset());
                }
            }
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    /**
     * Evaluates the given script body (or file) using the current session, context, and flowfile. The script
     * evaluation expects a FlowFile to be returned, in which case it will route the FlowFile to success. If a script
     * error occurs, the original FlowFile will be routed to failure. If the script succeeds but does not return a
     * FlowFile, the original FlowFile will be routed to no-flowfile
     *
     * @param context        the current process context
     * @param sessionFactory provides access to a {@link ProcessSessionFactory}, which
     *                       can be used for accessing FlowFiles, etc.
     * @throws ProcessException if the scripted processor's onTrigger() method throws an exception
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
            }
        }
        ScriptEngine scriptEngine = scriptingComponentHelper.engineQ.poll();
        ComponentLog log = getLogger();
        if (scriptEngine == null) {
            // No engine available so nothing more to do here
            return;
        }
        ProcessSession session = sessionFactory.createSession();
        try {

            try {
                Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
                if (bindings == null) {
                    bindings = new SimpleBindings();
                }
                bindings.put("session", session);
                bindings.put("context", context);
                bindings.put("log", log);
                bindings.put("REL_SUCCESS", REL_SUCCESS);
                bindings.put("REL_FAILURE", REL_FAILURE);

                // Find the user-added properties and set them on the script
                for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                    if (property.getKey().isDynamic()) {
                        // Add the dynamic property bound to its full PropertyValue to the script engine
                        if (property.getValue() != null) {
                            bindings.put(property.getKey().getName(), context.getProperty(property.getKey()));
                        }
                    }
                }

                scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

                // Execute any engine-specific configuration before the script is evaluated
                ScriptEngineConfigurator configurator =
                        scriptingComponentHelper.scriptEngineConfiguratorMap.get(scriptingComponentHelper.getScriptEngineName().toLowerCase());

                // Evaluate the script with the configurator (if it exists) or the engine
                if (configurator != null) {
                    configurator.eval(scriptEngine, scriptToRun, scriptingComponentHelper.getModules());
                } else {
                    scriptEngine.eval(scriptToRun);
                }

                // Commit this session for the user. This plus the outermost catch statement mimics the behavior
                // of AbstractProcessor. This class doesn't extend AbstractProcessor in order to share a base
                // class with InvokeScriptedProcessor
                session.commit();
            } catch (ScriptException e) {
                // The below 'session.rollback(true)' reverts any changes made during this session (all FlowFiles are
                // restored back to their initial session state and back to their original queues after being penalized).
                // However if the incoming relationship is full of flow files, this processor will keep failing and could
                // cause resource exhaustion. In case a user does not want to yield, it can be set to 0s in the processor
                // configuration.
                context.yield();
                throw new ProcessException(e);
            }
        } catch (final Throwable t) {
            // Mimic AbstractProcessor behavior here
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});

            // the rollback might not penalize the incoming flow file if the exception is thrown before the user gets
            // the flow file from the session binding (ff = session.get()).
            session.rollback(true);
            throw t;
        } finally {
            scriptingComponentHelper.engineQ.offer(scriptEngine);
        }
    }

    @OnStopped
    public void stop() {
        scriptingComponentHelper.stop();
    }
}
