package org.apache.nifi.documentation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.io.IOUtil;

public class DocGeneratorTest {

    @Test
    public void testProcessorLoadsNarResources() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        System.out.println("temp folder: " + temporaryFolder.getRoot());
        
        NiFiProperties properties = loadSpecifiedProperties("/conf/nifi.properties");
        properties.setProperty(NiFiProperties.COMPONENT_DOCS_DIRECTORY, temporaryFolder.getRoot().getAbsolutePath());

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);

        DocGenerator.generate(properties);      

        File processorDirectory = new File(temporaryFolder.getRoot(), "org.apache.nifi.TestProcessor");
        File indexHtml = new File(processorDirectory, "index.html");
        
        String generatedHtml = FileUtils.readFileToString(indexHtml);
        Assert.assertNotNull(generatedHtml);
    }
    
    private static String toString(String fileName) {
        return fileName;
    }
    
    private NiFiProperties loadSpecifiedProperties(String propertiesFile) {
        String file = DocGeneratorTest.class.getResource(propertiesFile).getFile();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, file);

        NiFiProperties properties = NiFiProperties.getInstance();

        // clear out existing properties
        for (String prop : properties.stringPropertyNames()) {
            properties.remove(prop);
        }

        InputStream inStream = null;
        try {
            inStream = new BufferedInputStream(new FileInputStream(file));
            properties.load(inStream);
        } catch (final Exception ex) {
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (final Exception ex) {
                    /**
                     * do nothing *
                     */
                }
            }
        }

        return properties;
    }
}
