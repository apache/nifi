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

import static javax.xml.xpath.XPathConstants.NODESET;
import static javax.xml.xpath.XPathConstants.STRING;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
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

import javax.xml.namespace.QName;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.xpath.XPathEvaluator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more XPaths against the content of a FlowFile. The results of those XPaths are assigned to "
        + "FlowFile Attributes or are written to the content of the FlowFile itself, depending on configuration of the "
        + "Processor. XPaths are entered by adding user-defined properties; the name of the property maps to the Attribute "
        + "Name into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
        + "The value of the property must be a valid XPath expression. If the XPath evaluates to more than one node and the Return Type is "
        + "set to 'nodeset' (either directly, or via 'auto-detect' with a Destination of "
        + "'flowfile-content'), the FlowFile will be unmodified and will be routed to failure. If the XPath does not "
        + "evaluate to a Node, the FlowFile will be routed to 'unmatched' without having its contents modified. If Destination is "
        + "flowfile-attribute and the expression matches nothing, attributes will be created with empty strings as the value, and the "
        + "FlowFile will always be routed to 'matched'")
@WritesAttribute(attribute = "user-defined", description = "This processor adds user-defined attributes if the <Destination> property is set to flowfile-attribute.")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute'", value = "An XPath expression", description = "If <Destination>='flowfile-attribute' "
        + "then the FlowFile attribute is set to the result of the XPath Expression.  If <Destination>='flowfile-content' then the FlowFile content is set to the result of the XPath Expression.")
public class EvaluateXPath extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_NODESET = "nodeset";
    public static final String RETURN_TYPE_STRING = "string";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the XPath evaluation are written to the FlowFile content or a FlowFile attribute; "
                    + "if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one XPath may be specified, "
                    + "and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type")
            .description("Indicates the desired return type of the Xpath expressions.  Selecting 'auto-detect' will set the return type to 'nodeset' "
                    + "for a Destination of 'flowfile-content', and 'string' for a Destination of 'flowfile-attribute'.")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_NODESET, RETURN_TYPE_STRING)
            .defaultValue(RETURN_TYPE_AUTO)
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
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath is successfully evaluated and the FlowFile is modified as a result")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath does not match the content of the FlowFile and the Destination is set to flowfile-content")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid XML, or if the Return "
                    + "Type is 'nodeset' and the XPath evaluates to multiple nodes")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<XPathFactory> factoryRef = new AtomicReference<>();

    static {
        System.setProperty("javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON, "net.sf.saxon.xpath.XPathFactoryImpl");
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(RETURN_TYPE);
        properties.add(VALIDATE_DTD);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int xpathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    xpathCount++;
                }
            }

            if (xpathCount != 1) {
                results.add(new ValidationResult.Builder().subject("XPaths").valid(false)
                        .explanation("Exactly one XPath must be set if using destination of " + DESTINATION_CONTENT).build());
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

    @OnScheduled
    public void initializeXPathFactory() throws XPathFactoryConfigurationException {
        factoryRef.set(XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON));
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName).expressionLanguageSupported(false)
                .addValidator(new XPathValidator()).required(false).dynamic(true).build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
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

        final XPathFactory factory = factoryRef.get();
        final XPathEvaluator xpathEvaluator = (XPathEvaluator) factory.newXPath();
        final Map<String, XPathExpression> attributeToXPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final XPathExpression xpathExpression;
            try {
                xpathExpression = xpathEvaluator.compile(entry.getValue());
                attributeToXPathMap.put(entry.getKey().getName(), xpathExpression);
            } catch (XPathExpressionException e) {
                throw new ProcessException(e);  // should not happen because we've already validated the XPath (in XPathValidator)
            }
        }

        final XPathExpression slashExpression;
        try {
            slashExpression = xpathEvaluator.compile("/");
        } catch (XPathExpressionException e) {
            logger.error("unable to compile XPath expression due to {}", new Object[]{e});
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        final String destination = context.getProperty(DESTINATION).getValue();
        final QName returnType;

        switch (context.getProperty(RETURN_TYPE).getValue()) {
            case RETURN_TYPE_AUTO:
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    returnType = STRING;
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    returnType = NODESET;
                } else {
                    throw new IllegalStateException("The only possible destinations should be CONTENT or ATTRIBUTE...");
                }
                break;
            case RETURN_TYPE_NODESET:
                returnType = NODESET;
                break;
            case RETURN_TYPE_STRING:
                returnType = STRING;
                break;
            default:
                throw new IllegalStateException("There are no other return types...");
        }

        flowFileLoop:
        for (FlowFile flowFile : flowFiles) {
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<Source> sourceRef = new AtomicReference<>(null);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final List<Source> rootList = (List<Source>) slashExpression.evaluate(new SAXSource(xmlReader,
                                new InputSource(in)), NODESET);
                        sourceRef.set(rootList.get(0));
                    } catch (final Exception e) {
                        error.set(e);
                    }
                }
            });

            if (error.get() != null) {
                logger.error("unable to evaluate XPath against {} due to {}; routing to 'failure'",
                        new Object[]{flowFile, error.get()});
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> xpathResults = new HashMap<>();

            for (final Map.Entry<String, XPathExpression> entry : attributeToXPathMap.entrySet()) {
                Object result = null;
                try {
                    result = entry.getValue().evaluate(sourceRef.get(), returnType);
                    if (result == null) {
                        continue;
                    }
                } catch (final XPathExpressionException e) {
                    logger.error("failed to evaluate XPath for {} for Property {} due to {}; routing to failure",
                            new Object[]{flowFile, entry.getKey(), e});
                    session.transfer(flowFile, REL_FAILURE);
                    continue flowFileLoop;
                }

                if (returnType == NODESET) {
                    List<Source> nodeList = (List<Source>) result;
                    if (nodeList.isEmpty()) {
                        logger.info("Routing {} to 'unmatched'", new Object[]{flowFile});
                        session.transfer(flowFile, REL_NO_MATCH);
                        continue flowFileLoop;
                    } else if (nodeList.size() > 1) {
                        logger.error("Routing {} to 'failure' because the XPath evaluated to {} XML nodes",
                                new Object[]{flowFile, nodeList.size()});
                        session.transfer(flowFile, REL_FAILURE);
                        continue flowFileLoop;
                    }
                    final Source sourceNode = nodeList.get(0);

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        try {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            doTransform(sourceNode, baos);
                            xpathResults.put(entry.getKey(), baos.toString("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            throw new ProcessException(e);
                        } catch (TransformerException e) {
                            error.set(e);
                        }

                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream rawOut) throws IOException {
                                try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                    doTransform(sourceNode, out);
                                } catch (TransformerException e) {
                                    error.set(e);
                                }
                            }
                        });
                    }

                } else if (returnType == STRING) {
                    final String resultString = (String) result;

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        xpathResults.put(entry.getKey(), resultString);
                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream rawOut) throws IOException {
                                try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                    out.write(resultString.getBytes("UTF-8"));
                                }
                            }
                        });
                    }
                }
            }

            if (error.get() == null) {
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    flowFile = session.putAllAttributes(flowFile, xpathResults);
                    final Relationship destRel = xpathResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                    logger.info("Successfully evaluated XPaths against {} and found {} matches; routing to {}",
                            new Object[]{flowFile, xpathResults.size(), destRel.getName()});
                    session.transfer(flowFile, destRel);
                    session.getProvenanceReporter().modifyAttributes(flowFile);
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    logger.info("Successfully updated content for {}; routing to 'matched'", new Object[]{flowFile});
                    session.transfer(flowFile, REL_MATCH);
                    session.getProvenanceReporter().modifyContent(flowFile);
                }
            } else {
                logger.error("Failed to write XPath result for {} due to {}; routing original to 'failure'",
                        new Object[]{flowFile, error.get()});
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void doTransform(final Source sourceNode, OutputStream out) throws TransformerFactoryConfigurationError, TransformerException {
        final Transformer transformer;
        try {
            transformer = TransformerFactory.newInstance().newTransformer();
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        final Properties props = new Properties();
        props.setProperty(OutputKeys.METHOD, "xml");
        props.setProperty(OutputKeys.INDENT, "no");
        props.setProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperties(props);

        final ComponentLog logger = getLogger();

        final AtomicReference<TransformerException> error = new AtomicReference<>(null);
        transformer.setErrorListener(new ErrorListener() {
            @Override
            public void warning(final TransformerException exception) throws TransformerException {
                logger.warn("Encountered warning from XPath Engine: ", new Object[]{exception.toString(), exception});
            }

            @Override
            public void error(final TransformerException exception) throws TransformerException {
                logger.error("Encountered error from XPath Engine: ", new Object[]{exception.toString(), exception});
                error.set(exception);
            }

            @Override
            public void fatalError(final TransformerException exception) throws TransformerException {
                logger.error("Encountered warning from XPath Engine: ", new Object[]{exception.toString(), exception});
                error.set(exception);
            }
        });

        transformer.transform(sourceNode, new StreamResult(out));
        if (error.get() != null) {
            throw error.get();
        }
    }

    private static class XPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                XPathFactory factory = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
                final XPathEvaluator evaluator = (XPathEvaluator) factory.newXPath();

                String error = null;
                try {
                    evaluator.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }

                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XPath engine due to " + e.toString()).build();
            }
        }
    }
}
