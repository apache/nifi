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
package org.apache.nifi.documentation;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.html.HtmlDocumentationWriter;
import org.apache.nifi.documentation.html.HtmlProcessorDocumentationWriter;
import org.apache.nifi.documentation.html.HtmlPythonProcessorDocumentationWriter;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Enumerate available Components from Extension Manager and generate HTML documentation
 *
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);

    /**
     * Generates documentation into the work/docs dir specified by
     * NiFiProperties.
     *
     * @param properties to lookup nifi properties
     * @param extensionMapping extension mapping
     */
    public static void generate(final NiFiProperties properties, final ExtensionManager extensionManager, final ExtensionMapping extensionMapping) {
        final File explodedNiFiDocsDir = properties.getComponentDocumentationWorkingDirectory();

        logger.debug("Generating Documentation: Components [{}] Directory [{}]", extensionMapping.size(), explodedNiFiDocsDir);

        documentConfigurableComponent(extensionManager.getExtensions(Processor.class), explodedNiFiDocsDir, extensionManager);
        documentConfigurableComponent(extensionManager.getExtensions(ControllerService.class), explodedNiFiDocsDir, extensionManager);
        documentConfigurableComponent(extensionManager.getExtensions(ReportingTask.class), explodedNiFiDocsDir, extensionManager);
        documentConfigurableComponent(extensionManager.getExtensions(FlowAnalysisRule.class), explodedNiFiDocsDir, extensionManager);
        documentConfigurableComponent(extensionManager.getExtensions(ParameterProvider.class), explodedNiFiDocsDir, extensionManager);
    }

    /**
     * Documents a type of configurable component.
     *
     * @param extensionDefinitions definitions of the extensions to document
     * @param explodedNiFiDocsDir base directory of component documentation
     */
    public static void documentConfigurableComponent(final Set<ExtensionDefinition> extensionDefinitions, final File explodedNiFiDocsDir, final ExtensionManager extensionManager) {
        for (final ExtensionDefinition extensionDefinition : extensionDefinitions) {
            final Bundle bundle = extensionDefinition.getBundle();
            if (bundle == null) {
                logger.warn("Documentation generation failed: Extension bundle not found [{}]", extensionDefinition);
                continue;
            }

            final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();

            final String extensionClassName = extensionDefinition.getImplementationClassName();
            final String path = coordinate.getGroup() + "/" + coordinate.getId() + "/" + coordinate.getVersion() + "/" + extensionClassName;
            final File componentDirectory = new File(explodedNiFiDocsDir, path);
            final File indexHtml = new File(componentDirectory, "index.html");
            if (indexHtml.exists()) {
                // index.html already exists, no need to unpack the docs again.
                logger.debug("Existing documentation found [{}] skipped component class [{}]", indexHtml.getAbsolutePath(), extensionClassName);
                continue;
            }

            final Class<?> extensionType = extensionDefinition.getExtensionType();
            if (ConfigurableComponent.class.isAssignableFrom(extensionType)) {
                if (componentDirectory.mkdirs()) {
                    logger.debug("Documentation directory created [{}]", componentDirectory);
                }

                switch (extensionDefinition.getRuntime()) {
                    case PYTHON -> {
                        final String componentClass = extensionDefinition.getImplementationClassName();
                        final PythonProcessorDetails processorDetails = extensionManager.getPythonProcessorDetails(componentClass, extensionDefinition.getVersion());
                        try {
                            documentPython(componentDirectory, processorDetails);
                        } catch (Exception e) {
                            logger.warn("Documentation generation failed: Component Class [{}]", componentClass, e);
                        }
                    }
                    case JAVA -> {
                        final Class<?> extensionClass = extensionManager.getClass(extensionDefinition);
                        final Class<? extends ConfigurableComponent> componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                        try {
                            logger.debug("Documentation generation started: Component Class [{}]", componentClass);
                            document(extensionManager, componentDirectory, componentClass, coordinate);
                        } catch (Exception e) {
                            logger.warn("Documentation generation failed: Component Class [{}]", componentClass, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Generates the documentation for a particular configurable component. Will
     * check to see if an "additionalDetails.html" file exists and will link
     * that from the generated documentation.
     *
     * @param componentDocsDir the component documentation directory
     * @param componentClass the class to document
     * @throws IOException ioe
     */
    private static void document(final ExtensionManager extensionManager,
                                 final File componentDocsDir,
                                 final Class<? extends ConfigurableComponent> componentClass,
                                 final BundleCoordinate bundleCoordinate)
            throws IOException {

        // use temp components from ExtensionManager which should always be populated before doc generation
        final String classType = componentClass.getCanonicalName();
        final ConfigurableComponent component = extensionManager.getTempComponent(classType, bundleCoordinate);

        final DocumentationWriter<ConfigurableComponent> writer = getDocumentWriter(extensionManager, componentClass);

        final File baseDocumentationFile = new File(componentDocsDir, "index.html");
        if (baseDocumentationFile.exists()) {
            logger.warn("Overwriting Component Documentation [{}]", baseDocumentationFile);
        }

        try (final OutputStream output = new BufferedOutputStream(Files.newOutputStream(baseDocumentationFile.toPath()))) {
            writer.write(component, output, hasAdditionalInfo(componentDocsDir));
        }
    }

    /**
     * Generates the documentation for a particular configurable component. Will
     * check to see if an "additionalDetails.html" file exists and will link
     * that from the generated documentation.
     *
     * @param componentDocsDir the component documentation directory
     * @param processorDetails the python processor to document
     * @throws IOException ioe
     */
    private static void documentPython(final File componentDocsDir, final PythonProcessorDetails processorDetails) throws IOException {
        final DocumentationWriter<PythonProcessorDetails> writer = new HtmlPythonProcessorDocumentationWriter();
        final File baseDocumentationFile = new File(componentDocsDir, "index.html");

        if (baseDocumentationFile.exists()) {
            logger.warn("Overwriting Component Documentation [{}]", baseDocumentationFile);
        }

        try (final OutputStream output = new BufferedOutputStream(Files.newOutputStream(baseDocumentationFile.toPath()))) {
            writer.write(processorDetails, output, hasAdditionalInfo(componentDocsDir));
        }
    }

    private static DocumentationWriter<ConfigurableComponent> getDocumentWriter(
            final ExtensionManager extensionManager,
            final Class<? extends ConfigurableComponent> componentClass
    ) {
        if (Processor.class.isAssignableFrom(componentClass)) {
            return new HtmlProcessorDocumentationWriter(extensionManager);
        } else {
            return new HtmlDocumentationWriter(extensionManager);
        }
    }

    private static boolean hasAdditionalInfo(File directory) {
        final Path additionalDetailsPath = directory.toPath().resolve(HtmlDocumentationWriter.ADDITIONAL_DETAILS_HTML);
        return Files.exists(additionalDetailsPath);
    }
}
