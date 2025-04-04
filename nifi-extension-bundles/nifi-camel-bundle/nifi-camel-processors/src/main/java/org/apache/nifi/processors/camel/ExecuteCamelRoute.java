package org.apache.nifi.processors.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.dsl.support.RouteBuilderLoaderSupport;
import org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader;
import org.apache.camel.dsl.xml.io.XmlRoutesBuilderLoader;
import org.apache.camel.main.Main;
import org.apache.camel.support.ResourceHelper;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CapabilityDescription("Integrates Apache Nifi v2 with Apache Camel v4. "
        + "In order to work correctly, the user must specify a valid Camel route in YAML or XML format which: "
        + "(1) Reads from \"direct:in\" endpoint; this is a static camel input endpoint which cannot be changed. "
        + "(2) Redirect valid flow files to one or more \"direct:$name\" endpoint(s); $name is a dynamic output endpoint which can be changed from the user. "
        + "(3) Writes non-valid flow files to \"direct:err\" endpoint; this is a static camel output endpoint which cannot be changed."
        + "\n"
        + "In \"direct:in\" endpoint the input flow file content and attributes will be written in the camel message body and headers, respectively. This endpoint should be used in the first camel \"from\" clause. "
        + "The valid flow files redirected to \"direct:$name\" relationships will be routed to dynamically created nifi relationship with the new content/headers set in the camel route, if changed (flow file attributes can be changed through the camel message headers and flow file content through the camel message body). "
        + "If a flow file is routed to the \"direct:err\" relationship, internally the nifi processor will throw an exception and will write the original flow file in the \"failure\" relationship. "
        + "WARNING: Flow files routed to \"direct:$name\" relationships whose body is not converted to an instance of \"java.io.InputStream\" (this can be done through the Camel \"convertBodyTo\" step) will be routed to \"failure\" relationship. ")
@Tags({"Camel", "Transform", "Produce"})
@WritesAttributes({
        @WritesAttribute(attribute = "camel.error.message", description = "This attribute will contain the exception stacktrace, if the flow file is routed to failure relationship. ")
})
public class ExecuteCamelRoute extends AbstractProcessor {
    static final String YAML_ROUTE_TYPE = "Yaml";
    static final String XML_ROUTE_TYPE = "Xml";
    private static final String outEndpointHeaderName = "outEndpoint";
    private final NifiCamelRoute defaultInRoute = new NifiCamelRoute("in");
    private final NifiCamelRoute defaultErrRoute = new NifiCamelRoute("err");
    private Main main;
    private ProducerTemplate template;

    public static final PropertyDescriptor CAMEL_ROUTE_TYPE = new PropertyDescriptor
            .Builder()
            .name("Camel Route Type")
            .displayName("Camel Route Type")
            .description("Camel Route Type: (1) Yaml: allows to specify route in yaml format; (2) Xml: allows to specify route in xml format")
            .required(true)
            .allowableValues(YAML_ROUTE_TYPE, XML_ROUTE_TYPE)
            .defaultValue(YAML_ROUTE_TYPE)
            .build();

    public static final PropertyDescriptor CAMEL_ROUTE = new PropertyDescriptor
            .Builder()
            .name("Camel Route")
            .displayName("Camel Route")
            .description("User supplied route in the specified DSL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private final HashMap<String, Relationship> relationships = new HashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(CAMEL_ROUTE_TYPE, CAMEL_ROUTE);
        initializeRelationships();
    }

    private void initializeRelationships(){
        relationships.clear();
        relationships.put(REL_FAILURE.getName(), REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(this.relationships.values());
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        main = new Main();
        main.start();

        while (!main.isStarted()) {
            getLogger().info("Camel is starting. Waiting 0.5 second...");
            Thread.sleep(500);
        }
        template = main.getCamelContext().createProducerTemplate();
        main.getCamelContext().addRoutes(new NifiCamelDefaultErrRouteBuilder(defaultErrRoute));

        try (
            RouteBuilderLoaderSupport loader = getRouteLoader(context)
        ) {
            String routeDefinition = context.getProperty(CAMEL_ROUTE).getValue();
            RoutesBuilder rb = loader.loadRoutesBuilder(ResourceHelper.fromString("route", routeDefinition));
            main.getCamelContext().addRoutes(rb);
            addCamelDynamicRoutes();
        }
    }

    private void addCamelDynamicRoutes() throws Exception {
        for (Map.Entry<String, Relationship> entry : relationships.entrySet()) {
            if (!entry.getKey().equals(REL_FAILURE.getName())) {
                String endpoint = entry.getKey();
                main.getCamelContext().addRoutes(new NifiCamelOutRouteBuilder(new NifiCamelRoute(endpoint), outEndpointHeaderName));
            }
        }
    }

    private RouteBuilderLoaderSupport getRouteLoader(ProcessContext context) {
        if (context.getProperty(CAMEL_ROUTE_TYPE).getValue().equals(YAML_ROUTE_TYPE)) {
            return new YamlRoutesBuilderLoader();
        } else {
            return new XmlRoutesBuilderLoader();
        }
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) throws RuntimeException {
        if (descriptor.equals(CAMEL_ROUTE)) {
            try {
                extractDynamicRelationships(newValue);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void extractDynamicRelationships(String routeDefinition) {
        initializeRelationships();
        Pattern pattern = Pattern.compile("direct:(\\w+)");
        Matcher matcher = pattern.matcher(routeDefinition);
        while (matcher.find()) {
            String endpoint = matcher.group(1);
            if (!endpoint.equalsIgnoreCase(defaultInRoute.getName()) && !endpoint.equalsIgnoreCase(defaultErrRoute.getName())) {
                Relationship rel = new Relationship.Builder()
                        .name(endpoint)
                        .description("Camel endpoint direct:" + endpoint)
                        .build();
                relationships.put(rel.getName(), rel);
            }
        }
    }

    @OnStopped
    public void onStop() {
        template.stop();
        main.stop();
        main.shutdown();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile newFlowFile = null;
        OutputStream os = null;
        try {
            newFlowFile = session.create(flowFile);
            os = session.write(newFlowFile);

            OutputStream flowFileOs = os;
            Exchange exchange = template.send(defaultInRoute.getUri(), e -> {
                e.getIn().setBody(getFlowFileContent(flowFile, session));
                e.getIn().setHeaders(new HashMap<>(flowFile.getAttributes()));
                e.setVariable(NifiCamelOutRouteBuilder.OUTPUT_STREAM_HEADER_NAME, flowFileOs);
            });
            Exception exception = exchange.getException();

            if (exception != null) {
                session.putAllAttributes(flowFile, camelHeadersToNifiAttributes(exchange.getMessage().getHeaders()));
                throw exception;
            } else {
                os.flush();
                os.close();
                session.putAllAttributes(newFlowFile, camelHeadersToNifiAttributes(exchange.getMessage().getHeaders()));
                session.transfer(newFlowFile, getOutRelationship(exchange.getIn()));
                session.remove(flowFile);
            }
        } catch (Exception e) {
            handleException(flowFile, newFlowFile, os, session, e);
        }
    }

    private Relationship getOutRelationship(Message message) {
        Object endpointName = message.getHeader(outEndpointHeaderName);
        if (endpointName == null) {
            throw new RuntimeException("No outgoing endpoint found in Camel exchange: some data is completing processing without entering a direct endpoint.");
        }
        Relationship outRel = relationships.get(endpointName.toString());
        if (outRel == null) {
            throw new RuntimeException("Internal error: dynamic relationship not found in relationships list " + relationships);
        }
        return outRel;
    }

    private void handleException(FlowFile flowFile, FlowFile newFlowFile, OutputStream os, ProcessSession session, Exception e) {
        if (os != null) {
            try {
                os.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        if (newFlowFile != null) {
            session.remove(newFlowFile);
        }
        session.transfer(getErrorFlowFile(e, flowFile, session), REL_FAILURE);
    }

    private Map<String, String> camelHeadersToNifiAttributes(Map<String, Object> headers) {
        Map<String, String> attributes = new HashMap<>();
        for (String k : headers.keySet()) {
            try {
                if (!NifiCamelOutRouteBuilder.OUTPUT_STREAM_HEADER_NAME.equals(k) && !outEndpointHeaderName.equals(k)) {
                    attributes.put(k, headers.get(k).toString());
                }
            } catch (Exception ignored) {
                getLogger().warn("Unable to convert " + k + " camel header to String");
            }
        }
        return attributes;
    }

    private FlowFile getErrorFlowFile(Exception e, FlowFile flowFile, ProcessSession session) {
        StringWriter stringWriter = new StringWriter();
        if (e.getCause() == null) {
            e.printStackTrace(new PrintWriter(stringWriter));
        } else {
            e.getCause().printStackTrace(new PrintWriter(stringWriter));
        }
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("camel.error.message", StringUtils.truncateString(stringWriter.toString().trim(), 1024));
        return session.putAllAttributes(flowFile, attributes);
    }

    private ByteArrayOutputStream getFlowFileContent(FlowFile flowFile, ProcessSession session) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        return bytes;
    }

}
