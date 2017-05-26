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
package org.apache.nifi.processors.xmlcalabash;

import org.apache.commons.io.IOUtils;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.xml.sax.InputSource;

import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

import com.xmlcalabash.core.XProcException;
import com.xmlcalabash.io.ReadablePipe;
import com.xmlcalabash.io.WritableDocument;
import com.xmlcalabash.model.RuntimeValue;
import com.xmlcalabash.model.Serialization;
import com.xmlcalabash.runtime.XPipeline;
import com.xmlcalabash.util.Input;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"XML, XProc, XMLCalabash"})
@CapabilityDescription(
    "Inserts a FlowFile into a specified XProc XML pipeline, allowing one to perform " +
    "complex validations and transformations of XML data within a single NiFi " +
    "processor. This processor provides a FlowFile to the primary input port of the " +
    "XProc pipeline. It is an error if the XProc pipeline does not define a primary " +
    "input port. When data exits the XProc pipeline via one or more output ports, a " +
    "FlowFile is created and transferred to a dynamic NiFi relationship having the " +
    "same name as the output port. If a failure occurs during XProc processing, the " +
    "original FlowFile is transferred to the 'pipeline failure' relationship and " +
    "nothing transferred to any of the the dynamic output port relationships. " +
    "Dynamic properties may be defined, with their names and values passed to the " +
    "XProc pipeline as XProc options. Note that all input and output XML data " +
    "reside in memory during XML pipeline processing. Also note that due the " +
    "non-thread safe library used for pipeline processing, a pool of pipeline instances " +
    "is created, with the pool size defined by the number of maximum concurrent tasks. " +
    "If memory usage is a concern, the number of concurrent tasks should be reduced, " +
    "thus limiting the number of XML Calabash pipeline instances created and XML data " +
    "stored in memory. For more information on XProc, visit http://www.w3.org/TR/xproc/")
@DynamicProperty(
    name = "XProc Option Name",
    value = "XProc Option Value",
    supportsExpressionLanguage = true,
    description = "Option names and values passed to the XProc pipeline. The dynamic " +
        "property name must be in Clark-notation, {uri}name, though the {uri} " +
        "prefix may be optional depending on the XProc file. The property name " +
        "is passed directly to the XProc engine as an option name along with its " +
        "associated value.")
@DynamicRelationship(
    name = "XProc Output Port",
    description =
        "A dynamic relationship is created for each output port defined in the XProc " +
        "pipeline. When XML is written to an XProc output port, a FlowFile is created " +
        "for the XML, which is transferred to the relationship of the same name. Based on " +
        "the XProc pipeline, a single input FlowFile could result in outputs to more " +
        "than one relationship. If an XProc output port specifies sequence='true', then " +
        "multiple FlowFiles could be transferred to the same output relationship for a " +
        "single input FlowFile.")
@WritesAttributes({
    @WritesAttribute(attribute = "fragment.identifier",
        description = "All outputs produced from the same input FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index",
        description = "A one-up number that indicates the ordering of output port FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count",
        description = "The total number of FlowFiles generated from the input FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ",
        description = "The filename of the input FlowFile")
})
public class PipelineXml extends AbstractProcessor {

    private String inputPort = null;
    private List<PropertyDescriptor> properties;
    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private BlockingQueue<PipelineXmlData> pipelinePool = null;

    private static String findPrimaryInputPort(XPipeline pipeline) {
        for (String port : pipeline.getInputs()) {
            final com.xmlcalabash.model.Input inputPort = pipeline.getDeclareStep().getInput(port);
            if (inputPort.getPrimary() && !inputPort.getParameterInput()) {
                return port;
            }
        }
        return null;
    }

    private static final Validator XML_PIPELINE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(input);

            PipelineXmlData pd = null;
            try {
                if (subject == XML_PIPELINE_CONFIG.getName()) {
                    pd = new PipelineXmlData(input, null); // base URI not needed for finding primary ports
                } else if (subject == XML_PIPELINE_FILE.getName()) {
                    pd = new PipelineXmlData(new Input(input));
                } else {
                    return builder.valid(false).explanation("Can only validate XML Pipeline Config or XML Pipeline File").build();
                }
            } catch (SaxonApiException | XProcException e) {
                return builder.valid(false).explanation("XProc pipeline is invalid: " + e).build();
            }

            final String inputPort = findPrimaryInputPort(pd.pipeline);
            if (inputPort == null) {
                return builder.valid(false).explanation("XProc pipeline must define a primary non-parameter input port").build();
            }

            return builder.valid(true).build();
        }
    };

    private static final Validator XPROC_OPTION_NAME_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String optName, String optValue, ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(optName).input(optValue);

            try {
                QName.fromClarkName(optName);
                builder.valid(true);
            } catch (IllegalArgumentException e) {
                builder.valid(false).explanation("Option name must be of the form {uri}local.");
            }

            return builder.build();
        }
    };

    private static final Validator BASE_URI_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String optName, String optValue, ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(optName).input(optValue);

            try {
                URI uri = new URI(optValue);
                if (uri.isAbsolute()) {
                    builder.valid(true);
                } else {
                    builder.valid(false).explanation("Base URI must be an absolute URI of the form <scheme>:<path>");
                }
            } catch (URISyntaxException e) {
                builder.valid(false).explanation("Base URi is not a valid URI: " + e.getMessage());
            }

            return builder.build();
        }
    };


    // note that the names of these properties and relationships intentionally
    // have spaces so they cannot conflict with XProc option and output port
    // names, which are restricted to QName's and NCName's
    public static final PropertyDescriptor XML_PIPELINE_FILE = new PropertyDescriptor.Builder()
            .name("xml pipeline file")
            .displayName("XML Pipeline File")
            .description("Full path to a file containing the XProc XML pipeline configuration. Only one of XML Pipeline File or XML Pipeline Config may be used.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .addValidator(XML_PIPELINE_VALIDATOR)
            .build();

    public static final PropertyDescriptor XML_PIPELINE_CONFIG = new PropertyDescriptor.Builder()
            .name("xml pipeline config")
            .displayName("XML Pipeline Config")
            .description("XProc XML pipeline configuration. Only one of XML Pipeline File or XML Pipeline Config may be used.")
            .required(false)
            .addValidator(Validator.VALID)
            .addValidator(XML_PIPELINE_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASE_URI = new PropertyDescriptor.Builder()
            .name("base uri")
            .displayName("Base URI")
            .description("An absolute URI to used to resolve relative URIs in the XProc pipeline configuration. Must provide a scheme (e.g. file:, http:) followed by an absolute path. For example, file://path/for/resolving/ or http://example.com/resolving. Only required when the XML Pipeline Config property is provided.")
            .required(false)
            .addValidator(BASE_URI_VALIDATOR)
            .build();

    public static final Relationship REL_PIPELINE_FAILURE = new Relationship.Builder()
        .name("pipeline failure")
        .description("FlowFiles that fail XProc processing are routed here")
        .build();
    public static final Relationship REL_PIPELINE_ORIGINAL = new Relationship.Builder()
        .name("original xml")
        .description("FlowFiles that successfully complete XProc processing are routed here")
        .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(XML_PIPELINE_FILE);
        properties.add(XML_PIPELINE_CONFIG);
        properties.add(BASE_URI);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> set = new HashSet<>();
        set.add(REL_PIPELINE_FAILURE);
        set.add(REL_PIPELINE_ORIGINAL);
        relationships = new AtomicReference<>(set);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .required(false)
                .addValidator(XPROC_OPTION_NAME_VALIDATOR)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        final Set<Relationship> newRelationships = new HashSet<>();
        newRelationships.add(REL_PIPELINE_FAILURE);
        newRelationships.add(REL_PIPELINE_ORIGINAL);

        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        final String pipelineFile = propertyMap.get(XML_PIPELINE_FILE);
        final String pipelineConfig = propertyMap.get(XML_PIPELINE_CONFIG);

        if (StringUtils.isEmpty(pipelineFile) == StringUtils.isEmpty(pipelineConfig)) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                "Exactly one of XML Pipeline File or XML Pipeline Config must be set").build());
            this.relationships.set(newRelationships);
            return results;
        }

        PipelineXmlData pd = null;
        try {
            if (!StringUtils.isEmpty(pipelineFile)) {
                pd = new PipelineXmlData(new Input(pipelineFile));
            } else {
                final String baseURI = propertyMap.get(BASE_URI);
                if (baseURI == null) {
                    results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Base URI is required when XML Pipeline Config is set").build());
                    this.relationships.set(newRelationships);
                    return results;
                }
                pd = new PipelineXmlData(pipelineConfig, new URI(baseURI));
            }
        } catch (Exception e) {
            // shouldn't be possible, everything used should have been validated in the individual validators
            results.add(new ValidationResult.Builder().valid(false).explanation(
                "Failed to parse pipeline data: " + e).build());
            this.relationships.set(newRelationships);
            return results;
        }

        inputPort = findPrimaryInputPort(pd.pipeline);

        // we know everything is valid, so lets add the new output port relationships
        final Set<String> outputPorts = pd.pipeline.getOutputs();
        for (final String outputPort: outputPorts) {
            final Relationship outputRel = new Relationship.Builder()
                .name(outputPort)
                .description("The XProc output port named '" + outputPort + "'")
                .build();
            newRelationships.add(outputRel);
        }

        this.relationships.set(newRelationships);

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws SaxonApiException, URISyntaxException {
        final String pipelineFile = context.getProperty(XML_PIPELINE_FILE).getValue();
        final String pipelineConfig = context.getProperty(XML_PIPELINE_CONFIG).getValue();
        final int pipelinePoolSize = context.getMaxConcurrentTasks();
        pipelinePool = new ArrayBlockingQueue<>(pipelinePoolSize);
        for (int i = 0; i < pipelinePoolSize; i++) {
            final PipelineXmlData pd;
            if (!StringUtils.isEmpty(pipelineFile)) {
                pd = new PipelineXmlData(new Input(pipelineFile));
            } else {
                final String baseURI = context.getProperty(BASE_URI).getValue();
                pd = new PipelineXmlData(pipelineConfig, new URI(baseURI));
            }
            pipelinePool.add(pd);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        pipelinePool.clear();
        pipelinePool = null;
    }

    private void handleInput(PipelineXmlData pd, ProcessContext context, FlowFile original, InputStream stream) throws SaxonApiException {
        XdmNode inputNode = pd.runtime.parse(new InputSource(stream));
        pd.pipeline.writeTo(inputPort, inputNode);

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final QName optName = QName.fromClarkName(entry.getKey().getName());
            final String value = context.newPropertyValue(entry.getValue()).evaluateAttributeExpressions(original).getValue();
            pd.pipeline.passOption(optName, new RuntimeValue(value));
        }
    }

    private void handleOutput(PipelineXmlData pd, ProcessSession session, FlowFile original, List<Map.Entry<Relationship, FlowFile>> outputs) throws SaxonApiException {
        int fragmentCount = 0;

        for (final Relationship rel: getRelationships()) {
            if (rel == REL_PIPELINE_FAILURE || rel == REL_PIPELINE_ORIGINAL) {
                continue;
            }

            final ReadablePipe rpipe = pd.pipeline.readFrom(rel.getName());
            final Serialization serial = pd.pipeline.getSerialization(rel.getName());

            while (rpipe.moreDocuments()) {
                final XdmNode node = rpipe.read();

                FlowFile outputFF = session.create(original);
                outputFF = session.write(outputFF, out -> {
                    final WritableDocument wd = new WritableDocument(pd.runtime, null, serial, out);
                    wd.write(node);
                });

                outputFF = session.putAttribute(outputFF, FRAGMENT_INDEX.key(), String.valueOf(fragmentCount++));
                outputs.add(new SimpleEntry(rel, outputFF));
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        PipelineXmlData pd = null;
        try {
            pd = pipelinePool.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        final FlowFile original = session.get();
        if (original == null) {
            pipelinePool.add(pd);
            return;
        }

        final List<Map.Entry<Relationship, FlowFile>> outputs = new ArrayList<>();

        final InputStream stream = session.read(original);
        try {
            pd.pipeline.reset();
            handleInput(pd, context, original, stream);
            pd.pipeline.run();
            handleOutput(pd, session, original, outputs);

            // We got here without any errors, transfor the original and output
            // flow files
            final String fragmentIdentifier = UUID.randomUUID().toString();
            final String numberOfOutputs = Integer.toString(outputs.size());
            final FlowFile originalToTransfer = copyAttributesToOriginal(session, original, fragmentIdentifier, outputs.size());
            session.transfer(originalToTransfer, REL_PIPELINE_ORIGINAL);
            for (Map.Entry<Relationship, FlowFile> entry : outputs) {
                final Relationship rel = entry.getKey();
                FlowFile ff = entry.getValue();
                ff = session.putAttribute(ff, FRAGMENT_COUNT.key(), numberOfOutputs);
                ff = session.putAttribute(ff, FRAGMENT_ID.key(), fragmentIdentifier);
                ff = session.putAttribute(ff, SEGMENT_ORIGINAL_FILENAME.key(), originalToTransfer.getAttribute(CoreAttributes.FILENAME.key()));
                session.transfer(ff, rel);
            }
        } catch (SaxonApiException | XProcException e) {
            // If we got here, there was an error and nothing was transferred.
            // So transfer the original file to the pipeline failure
            // relationship and remove any output flow files that we may have
            // created before hitting the error.
            getLogger().error("Failed to process {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_PIPELINE_FAILURE);
            for (Map.Entry<Relationship, FlowFile> entry : outputs) {
                session.remove(entry.getValue());
            }
        } finally {
            // add the pipeline data back to the pool. The pool should always
            // have enough capacity so this should never fail
            pipelinePool.add(pd);
            IOUtils.closeQuietly(stream);
        }
    }
}
