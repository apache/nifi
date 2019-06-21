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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Pattern;

@Tags({"script", "execute", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "clojure"})
@CapabilityDescription("Experimental - Executes a script given the flow file and a process session.  The script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the script. If the handling is incomplete or incorrect, the session will be rolled back. Experimental: "
        + "Impact of sustained usage not yet verified.")
@DynamicProperty(
        name = "A script engine property to update, or a dynamic relationship",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value "
                + "specified by the Dynamic Property's value")
@DynamicRelationship(
        name = "A relationship to add",
        description = "If a dynamic property starts with 'rel.', it is assumed to be the name of a dynamic "
        + "relationship to add. It must obey to the pattern '^rel\\.[a-zA-Z][a-zA-Z0-9_]*$'. All (dynamic) "
        + "relationships can be accessed in the script variable 'relationships', which is a Map<String, Relationship> "
        + "[name of relationship] -> relationship"
)
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
public class ExecuteScript extends AbstractSessionFactoryProcessor implements Searchable {

    public static final Relationship REL_SUCCESS = ScriptingComponentUtils.REL_SUCCESS;
    public static final Relationship REL_FAILURE = ScriptingComponentUtils.REL_FAILURE;
    public static final String DYNAMIC_RELATIONSHIP_PATTERN_AS_STRING = "^rel\\.[a-zA-Z_][a-zA-Z0-9]*$";
    private static final Pattern DYNAMIC_RELATIONSHIP_PATTERN = Pattern.compile(DYNAMIC_RELATIONSHIP_PATTERN_AS_STRING);
    private static final String DYNAMIC_RELATIONSHIP_PREFIX = "rel.";

    private String scriptToRun = null;
    volatile ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();

    private final Set<Relationship> relationships;
    private ComponentLog log;

    public ExecuteScript() {
        super();
        relationships = new ConcurrentSkipListSet<>();
        relationships.add(ExecuteScript.REL_SUCCESS);
        relationships.add(ExecuteScript.REL_FAILURE);
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        log = getLogger();
    }

    /**
     * Returns the valid relationships for this processor.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(relationships);
    }

    private Map<String, Relationship> getRelationshipsAsMap() {
        final Map<String, Relationship> relMap = new HashMap<>();
        for (final Relationship rel : relationships) {
            relMap.put(rel.getName(), rel);
        }
        return Collections.unmodifiableMap(relMap);
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
        final boolean isRelationship = propertyDescriptorName != null && propertyDescriptorName.startsWith(DYNAMIC_RELATIONSHIP_PREFIX);
        if (isRelationship) {
            if (!DYNAMIC_RELATIONSHIP_PATTERN.matcher(propertyDescriptorName).matches()) {
                log.warn("dyn. property for relationship is invalid: '{}'. accepted patterns: '{}'", new Object[]{propertyDescriptorName, DYNAMIC_RELATIONSHIP_PATTERN_AS_STRING});
                return new PropertyDescriptor.Builder()
                        .addValidator(new RelationshipInvalidator())
                        .dynamic(true)
                        .required(false)
                        .name(propertyDescriptorName)
                        .build();
            }
        }
        final Validator validator = isRelationship
                ? Validator.VALID
                : StandardValidators.NON_EMPTY_VALIDATOR;
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(validator)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true);
        if (isRelationship) {
            builder.description(String.format(
                    "This property adds the relationship '%s'",
                    propertyDescriptorName.substring(4)
            ));
        }
        return builder.build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        final String descriptorName = descriptor.getName();
        if (descriptorName.startsWith(DYNAMIC_RELATIONSHIP_PREFIX)) {
            if (!DYNAMIC_RELATIONSHIP_PATTERN.matcher(descriptorName).matches()) {
                return;
            }
            final String relationshipName = descriptorName.substring(4);
            if (newValue == null) {
                relationships.removeIf(r -> relationshipName.equals(r.getName()));
                log.debug("removing relationship {}", new Object[]{relationshipName});
                return;
            }
            final Relationship relationship = new Relationship.Builder()
                    .name(relationshipName)
                    .description(String.format("dynamic relationship %s", relationshipName))
                    .build();
            relationships.add(relationship);
            log.debug("added dynamic relationship '{}'", new Object[]{relationshipName});
        }
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
                bindings.put("relationships", getRelationshipsAsMap());

                // Find the user-added properties that don't reference dynamic relationships, and set them on the script
                for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                    if (property.getKey().isDynamic() && !property.getKey().getName().startsWith(DYNAMIC_RELATIONSHIP_PREFIX)) {
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

    @Override
    public Collection<SearchResult> search(SearchContext context) {
        Collection<SearchResult> results = new ArrayList<>();

        String term = context.getSearchTerm();

        String scriptFile = context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).evaluateAttributeExpressions().getValue();
        String script = context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue();

        if (StringUtils.isBlank(script)) {
            try {
                script = IOUtils.toString(new FileInputStream(scriptFile), "UTF-8");
            } catch (Exception e) {
                getLogger().error(String.format("Could not read from path %s", scriptFile), e);
                return results;
            }
        }

        Scanner scanner = new Scanner(script);
        int index = 1;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (StringUtils.containsIgnoreCase(line, term)) {
                String text = String.format("Matched script at line %d: %s", index, line);
                results.add(new SearchResult.Builder().label(text).match(term).build());
            }
            index++;
        }

        return results;
    }

    private static class RelationshipInvalidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext validationContext) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("invalid dyn. relationship specified")
                    .valid(false)
                    .build();
        }
    }

}
