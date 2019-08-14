package org.apache.nifi.stateless.core;

import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.util.LoggingXmlParserErrorHandler;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;


public class FlowXmlUtil {

    private static final URL FLOW_XSD_RESOURCE = FingerprintFactory.class.getResource("/FlowConfiguration.xsd");
    private static final Logger logger = LoggerFactory.getLogger(FlowXmlUtil.class);

    // https://github.com/apache/nifi/blob/4f50f30ad7329540a707be835c23acee86d4cfb4/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-framework-core/src/main/java/org/apache/nifi/controller/StandardFlowSynchronizer.java
    private static Document parseFlowBytes(final byte[] flow) throws FlowSerializationException {
        // create document by parsing proposed flow bytes
        try {
            // create validating document builder
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(FLOW_XSD_RESOURCE);
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(schema);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new LoggingXmlParserErrorHandler("Flow Configuration", logger));

            // parse flow
            return (flow == null || flow.length == 0) ? null : docBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            throw new FlowSerializationException(ex);
        }
    }

    public static Document readFlowFromDisk(final Path flowPath) throws IOException {
        if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
            throw new IOException(flowPath.toString() + " does not exist or is empty");
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
             final InputStream gzipIn = new GZIPInputStream(in)) {
            FileUtils.copy(gzipIn, baos);
        }

        return parseFlowBytes(baos.toByteArray());
    }
}