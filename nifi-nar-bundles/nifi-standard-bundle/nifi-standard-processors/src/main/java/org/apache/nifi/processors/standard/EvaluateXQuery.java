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
package org.apache.nifi.processors.standard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.w3c.dom.Document;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import net.sf.saxon.s9api.DOMDestination;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryEvaluator;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath", "XQuery"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "Evaluates one or more XQueries against the content of a FlowFile.  The results of those XQueries are assigned "
        + "to FlowFile Attributes or are written to the content of the FlowFile itself, depending on configuration of "
        + "the Processor.  XQueries are entered by adding user-defined properties; the name of the property maps to the "
        + "Attribute Name into which the result will be placed (if the Destination is 'flowfile-attribute'; otherwise, "
        + "the property name is ignored).  The value of the property must be a valid XQuery.  If the XQuery returns more "
        + "than one result, new attributes or FlowFiles (for Destinations of 'flowfile-attribute' or 'flowfile-content' "
        + "respectively) will be created for each result (attributes will have a '.n' one-up number appended to the "
        + "specified attribute name).  If any provided XQuery returns a result, the FlowFile(s) will be routed to "
        + "'matched'. If no provided XQuery returns a result, the FlowFile will be routed to 'unmatched'.  If the "
        + "Destination is 'flowfile-attribute' and the XQueries matche nothing, no attributes will be applied to the "
        + "FlowFile.")
@WritesAttribute(attribute = "user-defined", description = "This processor adds user-defined attributes if the <Destination> property is set to flowfile-attribute .")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute'", value = "An XQuery", description = "If <Destination>='flowfile-attribute' "
        + "then the FlowFile attribute is set to the result of the XQuery.  If <Destination>='flowfile-content' then the FlowFile content is set to the result of the XQuery.")
public class EvaluateXQuery extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String OUTPUT_METHOD_XML = "xml";
    public static final String OUTPUT_METHOD_HTML = "html";
    public static final String OUTPUT_METHOD_TEXT = "text";

    public static final String UTF8 = "UTF-8";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description(
                    "Indicates whether the results of the XQuery evaluation are written to the FlowFile content or a "
                    + "FlowFile attribute. If set to <flowfile-content>, only one XQuery may be specified and the property "
                    + "name is ignored.  If set to <flowfile-attribute> and the XQuery returns more than one result, "
                    + "multiple attributes will be added to theFlowFile, each named with a '.n' one-up number appended to "
                    + "the specified attribute name")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor XML_OUTPUT_METHOD = new PropertyDescriptor.Builder()
            .name("Output: Method")
            .description("Identifies the overall method that should be used for outputting a result tree.")
            .required(true)
            .allowableValues(OUTPUT_METHOD_XML, OUTPUT_METHOD_HTML, OUTPUT_METHOD_TEXT)
            .defaultValue(OUTPUT_METHOD_XML)
            .build();

    public static final PropertyDescriptor XML_OUTPUT_OMIT_XML_DECLARATION = new PropertyDescriptor.Builder()
            .name("Output: Omit XML Declaration")
            .description("Specifies whether the processor should output an XML declaration when transforming a result tree.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor XML_OUTPUT_INDENT = new PropertyDescriptor.Builder()
            .name("Output: Indent")
            .description("Specifies whether the processor may add additional whitespace when outputting a result tree.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor VALIDATE_DTD = new PropertyDescriptor.Builder()
            .name("Validate DTD")
            .description("Specifies whether or not the XML content should be validated against the DTD.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the XQuery is successfully evaluated and the FlowFile "
                    + "is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when the XQuery does not match the content of the FlowFile "
                    + "and the Destination is set to flowfile-content")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when the XQuery cannot be evaluated against the content of "
                    + "the FlowFile.")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(XML_OUTPUT_METHOD);
        properties.add(XML_OUTPUT_OMIT_XML_DECLARATION);
        properties.add(XML_OUTPUT_INDENT);
        properties.add(VALIDATE_DTD);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int xQueryCount = 0;
            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    xQueryCount++;
                }
            }
            if (xQueryCount != 1) {
                results.add(new ValidationResult.Builder().subject("XQueries").valid(false)
                        .explanation("Exactly one XQuery must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }
        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).expressionLanguageSupported(false)
                .addValidator(new XQueryValidator()).required(false).dynamic(true).build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFileBatch = session.get(50);
        if (flowFileBatch.isEmpty()) {
            return;
        }
        final ComponentLog logger = getLogger();
        final Map<String, XQueryExecutable> attributeToXQueryMap = new HashMap<>();

        final Processor proc = new Processor(false);
        final XMLReader xmlReader;

        try {
            xmlReader = XMLReaderFactory.createXMLReader();
        } catch (SAXException e) {
            logger.error("Error while constructing XMLReader {}", new Object[]{e});
            throw new ProcessException(e.getMessage());
        }

        if (!context.getProperty(VALIDATE_DTD).asBoolean()) {
            xmlReader.setEntityResolver(new EntityResolver() {
                @Override
                public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
                    return new InputSource(new StringReader(""));
                }
            });
        }

        final XQueryCompiler comp = proc.newXQueryCompiler();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final XQueryExecutable exp;
            try {
                exp = comp.compile(entry.getValue());
                attributeToXQueryMap.put(entry.getKey().getName(), exp);
            } catch (SaxonApiException e) {
                throw new ProcessException(e);  // should not happen because we've already validated the XQuery (in XQueryValidator)
            }
        }

        final XQueryExecutable slashExpression;
        try {
            slashExpression = comp.compile("/");
        } catch (SaxonApiException e) {
            logger.error("unable to compile XQuery expression due to {}", new Object[]{e});
            session.transfer(flowFileBatch, REL_FAILURE);
            return;
        }

        final String destination = context.getProperty(DESTINATION).getValue();

        flowFileLoop:
        for (FlowFile flowFile : flowFileBatch) {
            if (!isScheduled()) {
                session.rollback();
                return;
            }

            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<XdmNode> sourceRef = new AtomicReference<>(null);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        XQueryEvaluator qe = slashExpression.load();
                        qe.setSource(new SAXSource(xmlReader, new InputSource(in)));
                        DocumentBuilderFactory dfactory = DocumentBuilderFactory.newInstance();
                        dfactory.setNamespaceAware(true);
                        Document dom = dfactory.newDocumentBuilder().newDocument();
                        qe.run(new DOMDestination(dom));
                        XdmNode rootNode = proc.newDocumentBuilder().wrap(dom);
                        sourceRef.set(rootNode);
                    } catch (final Exception e) {
                        error.set(e);
                    }
                }
            });

            if (error.get() != null) {
                logger.error("unable to evaluate XQuery against {} due to {}; routing to 'failure'",
                        new Object[]{flowFile, error.get()});
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> xQueryResults = new HashMap<>();
            List<FlowFile> childrenFlowFiles = new ArrayList<>();

            for (final Map.Entry<String, XQueryExecutable> entry : attributeToXQueryMap.entrySet()) {
                try {
                    XQueryEvaluator qe = entry.getValue().load();
                    qe.setContextItem(sourceRef.get());
                    XdmValue result = qe.evaluate();

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        int index = 1;
                        for (XdmItem item : result) {
                            String value = formatItem(item, context);
                            String attributeName = entry.getKey();
                            if (result.size() > 1) {
                                attributeName += "." + index++;
                            }
                            xQueryResults.put(attributeName, value);
                        }
                    } else { // if (DESTINATION_CONTENT.equals(destination)){
                        if (result.size() == 0) {
                            logger.info("Routing {} to 'unmatched'", new Object[]{flowFile});
                            session.transfer(flowFile, REL_NO_MATCH);
                            continue flowFileLoop;
                        } else if (result.size() == 1) {
                            final XdmItem item = result.itemAt(0);
                            flowFile = session.write(flowFile, new OutputStreamCallback() {
                                @Override
                                public void process(final OutputStream rawOut) throws IOException {
                                    try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                        writeformattedItem(item, context, out);
                                    } catch (TransformerFactoryConfigurationError | TransformerException e) {
                                        throw new IOException(e);
                                    }
                                }
                            });
                        } else {
                            for (final XdmItem item : result) {
                                FlowFile ff = session.clone(flowFile);
                                ff = session.write(ff, new OutputStreamCallback() {
                                    @Override
                                    public void process(final OutputStream rawOut) throws IOException {
                                        try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                            try {
                                                writeformattedItem(item, context, out);
                                            } catch (TransformerFactoryConfigurationError | TransformerException e) {
                                                throw new IOException(e);
                                            }
                                        }
                                    }
                                });
                                childrenFlowFiles.add(ff);
                            }
                        }
                    }
                } catch (final SaxonApiException e) {
                    logger.error("failed to evaluate XQuery for {} for Property {} due to {}; routing to failure",
                            new Object[]{flowFile, entry.getKey(), e});
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(childrenFlowFiles);
                    continue flowFileLoop;
                } catch (TransformerFactoryConfigurationError | TransformerException | IOException e) {
                    logger.error("Failed to write XQuery result for {} due to {}; routing original to 'failure'",
                            new Object[]{flowFile, error.get()});
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(childrenFlowFiles);
                    continue flowFileLoop;
                }
            }

            if (DESTINATION_ATTRIBUTE.equals(destination)) {
                flowFile = session.putAllAttributes(flowFile, xQueryResults);
                final Relationship destRel = xQueryResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                logger.info("Successfully evaluated XQueries against {} and found {} matches; routing to {}",
                        new Object[]{flowFile, xQueryResults.size(), destRel.getName()});
                session.transfer(flowFile, destRel);
                session.getProvenanceReporter().modifyAttributes(flowFile);
            } else { // if (DESTINATION_CONTENT.equals(destination)) {
                if (!childrenFlowFiles.isEmpty()) {
                    logger.info("Successfully created {} new FlowFiles from {}; routing all to 'matched'",
                            new Object[]{childrenFlowFiles.size(), flowFile});
                    session.transfer(childrenFlowFiles, REL_MATCH);
                    session.remove(flowFile);
                } else {
                    logger.info("Successfully updated content for {}; routing to 'matched'", new Object[]{flowFile});
                    session.transfer(flowFile, REL_MATCH);
                    session.getProvenanceReporter().modifyContent(flowFile);
                }
            }
        } // end flowFileLoop
    }

    private String formatItem(XdmItem item, ProcessContext context) throws TransformerConfigurationException, TransformerFactoryConfigurationError, TransformerException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeformattedItem(item, context, baos);
        return baos.toString();
    }

    void writeformattedItem(XdmItem item, ProcessContext context, OutputStream out)
            throws TransformerConfigurationException, TransformerFactoryConfigurationError, TransformerException, IOException {

        if (item.isAtomicValue()) {
            out.write(item.getStringValue().getBytes(UTF8));
        } else { // item is an XdmNode
            XdmNode node = (XdmNode) item;
            switch (node.getNodeKind()) {
                case DOCUMENT:
                case ELEMENT:
                    Transformer transformer = TransformerFactory.newInstance().newTransformer();
                    final Properties props = getTransformerProperties(context);
                    transformer.setOutputProperties(props);
                    transformer.transform(node.asSource(), new StreamResult(out));
                    break;
                default:
                    out.write(node.getStringValue().getBytes(UTF8));
            }
        }
    }

    private Properties getTransformerProperties(ProcessContext context) {
        final String method = context.getProperty(XML_OUTPUT_METHOD).getValue();
        boolean indent = context.getProperty(XML_OUTPUT_INDENT).asBoolean();
        boolean omitDeclaration = context.getProperty(XML_OUTPUT_OMIT_XML_DECLARATION).asBoolean();
        return getTransformerProperties(method, indent, omitDeclaration);
    }

    static Properties getTransformerProperties(final String method, final boolean indent, final boolean omitDeclaration) {
        final Properties props = new Properties();
        props.setProperty(OutputKeys.METHOD, method);
        props.setProperty(OutputKeys.INDENT, indent ? "yes" : "no");
        props.setProperty(OutputKeys.OMIT_XML_DECLARATION, omitDeclaration ? "yes" : "no");
        return props;
    }

    private static class XQueryValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                final Processor proc = new Processor(false);
                final XQueryCompiler comp = proc.newXQueryCompiler();
                String error = null;
                try {
                    comp.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }
                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XQuery engine due to " + e.toString()).build();
            }
        }
    }
}
