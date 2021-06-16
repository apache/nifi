/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.headless;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.util.FlowParser;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlowEnricher {

    private static final Logger logger = LoggerFactory.getLogger(FlowEnricher.class);

    private final HeadlessNiFiServer headlessNiFiServer;
    private final FlowParser flowParser;
    private final NiFiProperties niFiProperties;

    public static final String PROCESSOR_TAG_NAME = "processor";
    public static final String CONTROLLER_SERVICE_TAG_NAME = "controllerService";
    public static final String REPORTING_TASK_TAG_NAME = "reportingTask";

    public FlowEnricher(HeadlessNiFiServer headlessNiFiServer, FlowParser flowParser, NiFiProperties niFiProperties) {
        this.headlessNiFiServer = headlessNiFiServer;
        this.flowParser = flowParser;
        this.niFiProperties = niFiProperties;
    }

    /**
     * Traverse a flow document and enrich all components with bundle pairings that satisfy the constraints presented by
     * the versions of bundles supplied on the classpath.
     * <p>
     * The primary nature of these relationships is comprised of a standlone instance
     *
     * @throws FlowEnrichmentException if the provided flow cannot be enriched
     */
    public void enrichFlowWithBundleInformation() throws FlowEnrichmentException {
        final Path flowPath = niFiProperties.getFlowConfigurationFile().toPath();
        logger.debug("Enriching generated {} with bundling information", flowPath.toAbsolutePath());

        try {
            // Prepare elements and establish initial bookkeeping to use for analysis
            final Document flowDocument = flowParser.parseDocument(flowPath.toAbsolutePath().toFile());

            if (flowDocument == null) {
                throw new FlowEnrichmentException("Unable to successfully parse the specified flow at " + flowPath.toAbsolutePath());
            }

            // Aggregate all dependency mappings of all component types that need to have a bundle evaluated with their
            // associated XML information
            final Map<String, EnrichingElementAdapter> componentEnrichingMap = new HashMap<>();

            // Treat all component types as one map
            for (String typeElementName : Arrays.asList(PROCESSOR_TAG_NAME, CONTROLLER_SERVICE_TAG_NAME, REPORTING_TASK_TAG_NAME)) {
                final NodeList componentNodeList = flowDocument.getElementsByTagName(typeElementName);
                final Map<String, EnrichingElementAdapter> elementIdToMetadataMap = mapComponents(componentNodeList);

                componentEnrichingMap.putAll(elementIdToMetadataMap);

            }

            // For each of the components we have, evaluate its dependencies and apply versions
            for (Map.Entry<String, EnrichingElementAdapter> componentIdToMetadata : componentEnrichingMap.entrySet()) {
                // If this particular component has already had bundle information applied, skip it
                final EnrichingElementAdapter componentToEnrich = componentIdToMetadata.getValue();
                if (componentToEnrich.getBundleElement() != null) {
                    continue;
                }

                final String componentToEnrichClass = componentToEnrich.getComponentClass();
                final Map<String, Bundle> componentToEnrichVersionToBundles = headlessNiFiServer.getBundles(componentToEnrichClass)
                        .stream()
                        .collect(Collectors.toMap(bundle -> bundle.getBundleDetails().getCoordinate().getVersion(), bundle -> bundle));

                enrichComponent(componentToEnrich, componentToEnrichVersionToBundles);
                // verify error conditions
            }

            flowParser.writeFlow(flowDocument, flowPath.toAbsolutePath());
        } catch (IOException | TransformerException e) {
            throw new FlowEnrichmentException("Unable to successfully automate the enrichment of the generated flow with bundle information", e);
        }
    }

    private void enrichComponent(EnrichingElementAdapter componentToEnrich, Map<String, Bundle> componentToEnrichVersionToBundles) throws FlowEnrichmentException {

        if (componentToEnrich.getBundleElement() != null) {
            return;
        }

        BundleCoordinate enrichingBundleCoordinate = null;
        if (!componentToEnrichVersionToBundles.isEmpty()) {
            // If there is only one supporting bundle, choose it, otherwise carry out additional analysis
            if (componentToEnrichVersionToBundles.size() == 1) {
                BundleDetails enrichingBundleDetails = componentToEnrichVersionToBundles.entrySet().iterator().next().getValue().getBundleDetails();
                enrichingBundleCoordinate = enrichingBundleDetails.getCoordinate();
                // Adjust the bundle to reflect the values we learned from the Extension Manager
                componentToEnrich.setBundleInformation(enrichingBundleCoordinate);
                componentToEnrich.setDependsUponBundleCoordinate(enrichingBundleDetails.getDependencyCoordinate());
            } else {
                // multiple options
                final Set<String> componentToEnrichBundleVersions = componentToEnrichVersionToBundles.values().stream()
                        .map(bundle -> bundle.getBundleDetails().getCoordinate().getVersion()).collect(Collectors.toSet());
                // Select the last version of those available for the enriching bundle
                final String bundleVersion = componentToEnrichBundleVersions.stream().sorted().reduce((version, otherVersion) -> otherVersion).get();
                final BundleCoordinate enrichingCoordinate = componentToEnrichVersionToBundles.get(bundleVersion).getBundleDetails().getCoordinate();
                componentToEnrich.setBundleInformation(enrichingCoordinate);
                logger.warn("Multiple enriching bundle options were available for component {}.  The automatically selected enriching bundle was {}",
                        new Object[]{componentToEnrich.getComponentClass(), enrichingCoordinate});
            }
        } else {
            logger.warn("Could not find any eligible bundles for {}.  Automatic start of the flow cannot be guaranteed.", componentToEnrich.getComponentClass());
        }
    }

    /**
     * Find dependent components for the nodes provided.
     * <p>
     * We do not have any other information in a generic sense other than that the  properties that make use of UUIDs
     * are eligible to be dependent components; there is no typing that a value is an ID and not just the format of a UUID.
     * If we find a property that has a UUID as its value, we take note and create a mapping.
     * If it is a valid ID of another component, we can use this to pair up versions, otherwise, it is ignored.
     *
     * @param parentNodes component nodes to map to dependent components (e.g. Processor -> Controller Service)
     * @return a map of component IDs to their metadata about their relationship
     */
    protected static Map<String, EnrichingElementAdapter> mapComponents(NodeList parentNodes) {
        final Map<String, EnrichingElementAdapter> componentReferenceMap = new HashMap<>();
        for (int compIdx = 0; compIdx < parentNodes.getLength(); compIdx++) {
            final Node subjComponent = parentNodes.item(compIdx);
            final EnrichingElementAdapter enrichingElement = new EnrichingElementAdapter((Element) subjComponent);
            componentReferenceMap.put(enrichingElement.getComponentId(), enrichingElement);
        }
        return componentReferenceMap;
    }


    /*
     * Convenience class to aid in interacting with the XML elements pertaining to a bundle-able component
     */
    public static class EnrichingElementAdapter {
        public static final String BUNDLE_ELEMENT_NAME = "bundle";

        public static final String GROUP_ELEMENT_NAME = "group";
        public static final String ARTIFACT_ELEMENT_NAME = "artifact";
        public static final String VERSION_ELEMENT_NAME = "version";

        public static final String PROPERTY_ELEMENT_NAME = "property";

        // Source object
        private Element rawElement;

        // Metadata
        private String id;
        private String compClass;
        private Element bundleElement;
        private BundleCoordinate dependsUponBundleCoordinate;

        public EnrichingElementAdapter(Element element) {
            this.rawElement = element;
        }

        public String getComponentId() {
            if (this.id == null) {
                this.id = lookupValue("id");
            }
            return this.id;
        }

        public String getComponentClass() {
            if (this.compClass == null) {
                this.compClass = lookupValue("class");
            }
            return compClass;
        }

        public Element getBundleElement() {
            if (this.bundleElement == null) {
                // Check if the raw element has bundle information, returning it if it does
                final NodeList bundleElements = this.rawElement.getElementsByTagName(BUNDLE_ELEMENT_NAME);
                if (bundleElements != null && bundleElements.getLength() == 1) {
                    this.bundleElement = (Element) bundleElements.item(0);
                }
            }
            return this.bundleElement;
        }

        public List<Element> getProperties() {
            return FlowParser.getChildrenByTagName(this.rawElement, PROPERTY_ELEMENT_NAME);
        }

        private String lookupValue(String elementName) {
            return FlowParser.getChildrenByTagName(this.rawElement, elementName).get(0).getTextContent();
        }

        public void setBundleInformation(final BundleCoordinate bundleCoordinate) {
            // If we are handling a component that does not yet have bundle information, create a placeholder element
            if (this.bundleElement == null) {
                this.bundleElement = this.rawElement.getOwnerDocument().createElement(BUNDLE_ELEMENT_NAME);
                for (String elementTag : Arrays.asList(GROUP_ELEMENT_NAME, ARTIFACT_ELEMENT_NAME, VERSION_ELEMENT_NAME)) {
                    this.bundleElement.appendChild(this.bundleElement.getOwnerDocument().createElement(elementTag));
                }
                this.rawElement.appendChild(this.bundleElement);
            }
            setBundleInformation(bundleCoordinate.getGroup(), bundleCoordinate.getId(), bundleCoordinate.getVersion());
        }

        private void setBundleInformation(String group, String artifact, String version) {
            this.bundleElement.getElementsByTagName(GROUP_ELEMENT_NAME).item(0).setTextContent(group);
            this.bundleElement.getElementsByTagName(ARTIFACT_ELEMENT_NAME).item(0).setTextContent(artifact);
            this.bundleElement.getElementsByTagName(VERSION_ELEMENT_NAME).item(0).setTextContent(version);
        }

        public void setDependsUponBundleCoordinate(BundleCoordinate dependsUponBundleCoordinate) {
            this.dependsUponBundleCoordinate = dependsUponBundleCoordinate;
        }

        private String getBundleElementPropertyContent(String elementName) {
            return (getBundleElement() == null) ? null : FlowParser.getChildrenByTagName(this.bundleElement, elementName).get(0).getTextContent();
        }

        public String getBundleGroup() {
            return getBundleElementPropertyContent(GROUP_ELEMENT_NAME);
        }

        public String getBundleId() {
            return getBundleElementPropertyContent(ARTIFACT_ELEMENT_NAME);
        }

        public String getBundleVersion() {
            return getBundleElementPropertyContent(VERSION_ELEMENT_NAME);
        }
    }
}
