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
package org.apache.nifi.processors.ccda;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.eclipse.emf.common.util.Diagnostic;
import org.openhealthtools.mdht.uml.cda.CDAPackage;
import org.openhealthtools.mdht.uml.cda.ClinicalDocument;
import org.openhealthtools.mdht.uml.cda.ccd.CCDPackage;
import org.openhealthtools.mdht.uml.cda.consol.ConsolPackage;
import org.openhealthtools.mdht.uml.cda.hitsp.HITSPPackage;
import org.openhealthtools.mdht.uml.cda.ihe.IHEPackage;
import org.openhealthtools.mdht.uml.cda.util.CDAUtil;
import org.openhealthtools.mdht.uml.cda.util.CDAUtil.ValidationHandler;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"CCDA", "healthcare", "extract", "attributes"})
@CapabilityDescription("Extracts information from an Consolidated CDA formatted FlowFile and provides individual attributes "
        + "as FlowFile attributes. The attributes are named as <Parent> <dot> <Key>. "
        + "If the Parent is repeating, the naming will be <Parent> <underscore> <Parent Index> <dot> <Key>. "
        + "For example, section.act_07.observation.name=Essential hypertension")
public class ExtractCCDAAttributes extends AbstractProcessor {

    private static final char FIELD_SEPARATOR = '@';
    private static final char KEY_VALUE_SEPARATOR = '#';

    private Map<String, Map<String, String>> processMap = new LinkedHashMap<String, Map<String, String>>(); // stores mapping data for Parser
    private JexlEngine jexl = null; // JEXL Engine to execute code for mapping
    private JexlContext jexlCtx = null; // JEXL Context to hold element being processed

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    /**
     * SKIP_VALIDATION - Indicates whether to validate the CDA document after loading.
     * if true and the document is not valid, then ProcessException will be thrown
     */
    public static final PropertyDescriptor SKIP_VALIDATION = new PropertyDescriptor.Builder().name("skip-validation")
            .displayName("Skip Validation").description("Whether or not to validate CDA message values").required(true)
            .allowableValues("true", "false").defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    /**
     * REL_SUCCESS - Value to be returned in case the processor succeeds
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship if it is properly parsed as CDA and its contents extracted as attributes.")
            .build();

    /**
     * REL_FAILURE - Value to be returned in case the processor fails
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as CDA or its contents extracted as attributes.")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(_relationships);

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(SKIP_VALIDATION);
        this.properties = Collections.unmodifiableList(_properties);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        getLogger().debug("Loading packages");
        final StopWatch stopWatch = new StopWatch(true);

        // Load required MDHT packages
        System.setProperty( "org.eclipse.emf.ecore.EPackage.Registry.INSTANCE",
                "org.eclipse.emf.ecore.impl.EPackageRegistryImpl" );
        CDAPackage.eINSTANCE.eClass();
        HITSPPackage.eINSTANCE.eClass();
        CCDPackage.eINSTANCE.eClass();
        ConsolPackage.eINSTANCE.eClass();
        IHEPackage.eINSTANCE.eClass();
        stopWatch.stop();
        getLogger().debug("Loaded packages in {}", new Object[] {stopWatch.getDuration(TimeUnit.MILLISECONDS)});

        // Initialize JEXL
        jexl = new JexlBuilder().cache(1024).debug(false).silent(true).strict(false).create();
        jexlCtx = new MapContext();

        getLogger().debug("Loading mappings");
        loadMappings(); // Load CDA mappings for parser

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        Map<String, String> attributes = new TreeMap<String, String>(); // stores CDA attributes
        getLogger().info("Processing CCDA");

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        if(processMap.isEmpty()) {
            getLogger().error("Process Mapping is not loaded");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Boolean skipValidation = context.getProperty(SKIP_VALIDATION).asBoolean();

        final StopWatch stopWatch = new StopWatch(true);

        ClinicalDocument cd = null;
        try {
            cd = loadDocument(session.read(flowFile), skipValidation); // Load and optionally validate CDA document
        } catch (ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        getLogger().debug("Loaded document for {} in {}", new Object[] {flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS)});

        getLogger().debug("Processing elements");
        processElement(null, cd, attributes); // Process CDA element using mapping data

        flowFile = session.putAllAttributes(flowFile, attributes);

        stopWatch.stop();
        getLogger().debug("Successfully processed {} in {}", new Object[] {flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS)});
        if(getLogger().isDebugEnabled()){
            for (Entry<String, String> entry : attributes.entrySet()) {
                getLogger().debug("Attribute: {}={}", new Object[] {entry.getKey(), entry.getValue()});
            }
        }

        session.transfer(flowFile, REL_SUCCESS);

    }

    /**
     * Process elements children based on the parser mapping.
     * Any String values are added to attributes
     * For List, the processList method is called to iterate and process
     * For an Object this method is called recursively
     * While adding to the attributes the key is prefixed by parent
     * @param parent       parent key for this element, used as a prefix for attribute key
     * @param element      element to be processed
     * @param attributes   map of attributes to populate
     * @return             map of processed data, value can contain String or Map of Strings
     */
    protected Map<String, Object> processElement(String parent, Object element, Map<String, String> attributes) {
        final StopWatch stopWatch = new StopWatch(true);

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        String name = element.getClass().getName();
        Map<String, String> jexlMap = processMap.get(name); // get JEXL mappings for this element

        if (jexlMap == null) {
            getLogger().warn("Missing mapping for element " + name);
            return null;
        }

        for (Entry<String, String> entry : jexlMap.entrySet()) { // evaluate JEXL for each child element
            jexlCtx.set("element", element);
            JexlExpression jexlExpr = jexl.createExpression(entry.getValue());
            Object value = jexlExpr.evaluate(jexlCtx);
            String key = entry.getKey();
            String prefix = parent != null ? parent + "." + key : key;
            addElement(map, prefix, key, value, attributes);
        }
        stopWatch.stop();
        getLogger().debug("Processed {} in {}", new Object[] {name, stopWatch.getDuration(TimeUnit.MILLISECONDS)});

        return map;
    }

    /**
     * Adds element to the attribute list based on the type
     * @param map       object map
     * @param prefix    parent key as prefix
     * @param key       element key
     * @param value     element value
     */
    protected Map<String, String> addElement(Map<String, Object> map, String prefix, String key, Object value, Map<String, String> attributes) {
        // if the value is a String, add it to final attribute list
        // else process it further until we have a String representation
        if (value instanceof String) {
            if(value != null && !((String) value).isEmpty()) {
                map.put(key, value);
                attributes.put(prefix, (String) value);
            }
        } else if (value instanceof List) {
            if(value != null && !((List) value).isEmpty()) {
                map.put(key, processList(prefix, (List) value, attributes));
            }
        } else if (value != null) { // process element further
            map.put(key, processElement(prefix, value, attributes));
        }
        return attributes;
    }

    /**
     * Iterate through the list and calls processElement to process each element
     * @param key          key used while calling processElement
     * @param value        value is the individual Object being processed
     * @param attributes   map of attributes to populate
     * @return             list of elements
     */
    protected List<Object> processList(String key, List value, Map<String, String> attributes) {
        List<Object> items = new ArrayList<Object>();
        String keyFormat = value.size() > 1 ? "%s_%02d" : "%s";
        for (Object item : value) { // iterate over all elements and process each element
            items.add(processElement(String.format(keyFormat, key, items.size() + 1), item, attributes));
        }
        return items;
    }

    protected ClinicalDocument loadDocument(InputStream inputStream, Boolean skipValidation) {
        ClinicalDocument cd = null;

        try {
            cd = CDAUtil.load(inputStream); // load CDA document
            if (!skipValidation && !CDAUtil.validate(cd, new CDAValidationHandler())) { //optional validation
                getLogger().error("Failed to validate CDA document");
                throw new ProcessException("Failed to validate CDA document");
            }
        } catch (Exception e) {
            getLogger().error("Failed to load CDA document", e);
            throw new ProcessException("Failed to load CDA document", e);
        }
        return cd;
    }

    protected void loadMappings() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        Properties mappings = new Properties();
        try (InputStream is = classloader.getResourceAsStream("mapping.properties")){
            mappings.load(is);
            // each child element is key#value and multiple elements are separated by @
            for (String property : mappings.stringPropertyNames()) {
                String[] variables = StringUtils.split(mappings.getProperty(property), FIELD_SEPARATOR);
                Map<String, String> map = new LinkedHashMap<String, String>();
                for (String variable : variables) {
                    String[] keyvalue = StringUtils.split(variable, KEY_VALUE_SEPARATOR);
                    map.put(keyvalue[0], keyvalue[1]);
                }
                processMap.put(property, map);
            }

        } catch (IOException e) {
            getLogger().error("Failed to load mappings", e);
            throw new ProcessException("Failed to load mappings", e);
        }

    }

    protected class CDAValidationHandler implements ValidationHandler {
        @Override
        public void handleError(Diagnostic diagnostic) {
            getLogger().error(new StringBuilder("ERROR: ").append(diagnostic.getMessage()).toString());
        }

        @Override
        public void handleWarning(Diagnostic diagnostic) {
            getLogger().warn(new StringBuilder("WARNING: ").append(diagnostic.getMessage()).toString());
        }

        @Override
        public void handleInfo(Diagnostic diagnostic) {
            getLogger().info(new StringBuilder("INFO: ").append(diagnostic.getMessage()).toString());
        }
    }

}
