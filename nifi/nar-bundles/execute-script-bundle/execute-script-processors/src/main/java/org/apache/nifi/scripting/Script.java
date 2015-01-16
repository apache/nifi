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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.Relationship;

/**
 * <p>
 * Base class for all scripts. In this framework, only ScriptEngines that
 * implement javax.script.Invocable are supported.
 *
 * </p>
 *
 */
public class Script {

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Destination of successfully created flow files")
            .build();
    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Destination of flow files when a error occurs in the script")
            .build();

    static final Set<Relationship> RELATIONSHIPS;

    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(FAIL_RELATIONSHIP);
        rels.add(SUCCESS_RELATIONSHIP);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    FlowFile flowFile = null;
    ScriptEngine engine = null;

    protected Map<String, String> properties = new HashMap<>();
    protected Relationship lastRoute = SUCCESS_RELATIONSHIP;
    protected ProcessorLog logger;
    protected String scriptFileName;
    protected Map<String, String> attributes = new HashMap<>();
    protected long flowFileSize = 0;
    protected long flowFileEntryDate = System.currentTimeMillis();

    // the following are needed due to an inadequate JavaScript ScriptEngine. It will not allow
    // subclassing a Java Class, only implementing a Java Interface. So, the syntax of JavaScript
    // scripts looks like subclassing, but actually is just constructing a Script instance and
    // passing in functions as args to the constructor. When we move to Nashorn JavaScript ScriptEngine
    // in Java 8, we can get rid of these and revert the subclasses of this class to abstract.
    protected Object propDescCallback;
    protected Object relationshipsCallback;
    protected Object validateCallback;
    protected Object exceptionRouteCallback;

    /**
     * Create a Script without any parameters
     */
    public Script() {
    }

    public Script(Object... callbacks) {
        for (Object callback : callbacks) {
            if (callback instanceof Map<?, ?>) {
                propDescCallback = propDescCallback == null && ((Map<?, ?>) callback).containsKey("getPropertyDescriptors") ? callback
                        : propDescCallback;
                relationshipsCallback = relationshipsCallback == null && ((Map<?, ?>) callback).containsKey("getRelationships") ? callback
                        : relationshipsCallback;
                validateCallback = validateCallback == null && ((Map<?, ?>) callback).containsKey("validate") ? callback : validateCallback;
                exceptionRouteCallback = exceptionRouteCallback == null && ((Map<?, ?>) callback).containsKey("getExceptionRoute") ? callback
                        : exceptionRouteCallback;
            }
        }
    }

    /**
     * Specify a set of properties with corresponding NiFi validators.
     *
     * Subclasses that do not override this method will still have access to all
     * properties via the "properties" field
     *
     * @return a list of PropertyDescriptors
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    @SuppressWarnings("unchecked")
    public List<PropertyDescriptor> getPropertyDescriptors() throws NoSuchMethodException, ScriptException {
        if (propDescCallback != null) {
            return (List<PropertyDescriptor>) ((Invocable) engine).invokeMethod(propDescCallback, "getPropertyDescriptors", (Object) null);
        }
        return Collections.emptyList();
    }

    /**
     * Specify a set of reasons why this processor should be invalid.
     *
     * Subclasses that do not override this method will depend only on
     * individual property validators as specified in
     * {@link #getPropertyDescriptors()}.
     *
     * @return a Collection of messages to display to the user, or an empty
     * Collection if the processor configuration is OK.
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    @SuppressWarnings("unchecked")
    public Collection<String> validate() throws NoSuchMethodException, ScriptException {
        if (validateCallback != null) {
            return (Collection<String>) ((Invocable) engine).invokeMethod(validateCallback, "validate", (Object) null);
        }
        return Collections.emptyList();
    }

    void setFlowFile(FlowFile ff) {
        flowFile = ff;
        if (null != ff) {
            // have to clone because ff.getAttributes is unmodifiable
            this.attributes = new HashMap<>(ff.getAttributes());
            this.flowFileSize = ff.getSize();
            this.flowFileEntryDate = ff.getEntryDate();
        }
    }

    void setProperties(Map<String, String> map) {
        properties = new HashMap<>(map);
    }

    /**
     * Required to access entire properties map -- Jython (at least) won't let
     * you read the member variable without a getter
     *
     * @return entire parameter map
     */
    // change back to protected when we get nashorn
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Get the named parameter. Some scripting languages make a method call
     * easier than accessing a member field, so this is a convenience method to
     * look up values in the properties field.
     *
     * @param key a hash key
     * @return the value pointed at by the key specified
     */
    public String getProperty(String key) {
        return properties.get(key);
    }

    /**
     * Name the various relationships by which a file can leave this processor.
     * Subclasses may override this method to change available relationships.
     *
     * @return a collection of relationship names
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    @SuppressWarnings("unchecked")
    public Collection<Relationship> getRelationships() throws NoSuchMethodException, ScriptException {
        if (relationshipsCallback != null) {
            return (Collection<Relationship>) ((Invocable) engine).invokeMethod(relationshipsCallback, "getRelationships", (Object) null);
        }
        return RELATIONSHIPS;
    }

    /**
     * Determine what do with a file that has just been processed.
     *
     * After a script runs its "read" or "write" method, it should update the
     * "lastRoute" field to specify the relationship to which the resulting file
     * will be sent.
     *
     * @return a relationship name
     */
    public Relationship getRoute() {
        return lastRoute;
    }

    // Required because of a potential issue in Rhino -- protected methods are visible in
    // subclasses but protected fields (like "lastRoute") are not
    // change back to protected when we get nashorn
    public void setRoute(Relationship route) {
        lastRoute = route;
    }

    /**
     * Determine where to send a file if an exception is thrown during
     * processing.
     *
     * Subclasses may override this method to use a different relationship, or
     * to determine the relationship dynamically. Returning null causes the file
     * to be deleted instead.
     *
     * Defaults to "failure".
     *
     * @return the name of the relationship to use in event of an exception, or
     * null to delete the file.
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public Relationship getExceptionRoute() throws NoSuchMethodException, ScriptException {
        if (exceptionRouteCallback != null) {
            return (Relationship) ((Invocable) engine).invokeMethod(exceptionRouteCallback, "getExceptionRoute", (Object) null);
        }
        return FAIL_RELATIONSHIP;
    }

    /*
     * Some scripting languages make a method call easier than accessing a member field, so this is a convenience method to get
     * the incoming flow file size.
     */
    // Change back to protected when we get nashorn
    public long getFlowFileSize() {
        return flowFileSize;
    }

    /*
     * Some scripting languages make a method call easier than accessing a member field, so this is a convenience method to get
     * entry date of the flow file.
     */
    // Change back to protected when we get nashorn
    public long getFlowFileEntryDate() {
        return flowFileEntryDate;
    }

    void setLogger(ProcessorLog logger) {
        this.logger = logger;
    }

    /*
     * Required so that scripts in some languages can read access the attribute. Jython (at least) won't let you read the member
     * variable without a getter
     */
    protected ProcessorLog getLogger() {
        return this.logger;
    }

    void setFileName(String scriptFileName) {
        this.scriptFileName = scriptFileName;
    }

    public String getFileName() {
        return this.scriptFileName;
    }

    // this one's public because it's needed by ExecuteScript to update the flow file's attributes AFTER processing is done
    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    /*
     * Some scripting languages make a method call easier than accessing a member field, so this is a convenience method to look
     * up values in the attributes field.
     */
    // Change back to protected when we get nashorn
    public String getAttribute(String key) {
        return this.attributes.get(key);
    }

    /*
     * Some scripting languages make a method call easier than accessing a member field, so this is a convenience method to set
     * key/value pairs in the attributes field.
     */
    // Change back to protected when we get nashorn
    public void setAttribute(String key, String value) {
        this.attributes.put(key, value);
    }

    void setEngine(ScriptEngine scriptEngine) {
        this.engine = scriptEngine;
    }

}
