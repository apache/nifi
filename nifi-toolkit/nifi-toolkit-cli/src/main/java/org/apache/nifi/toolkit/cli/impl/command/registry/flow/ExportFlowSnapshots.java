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
package org.apache.nifi.toolkit.cli.impl.command.registry.flow;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderFactoryException;
import org.apache.nifi.registry.provider.StandardProviderConfigurationContext;
import org.apache.nifi.registry.provider.StandardProviderFactory;
import org.apache.nifi.registry.provider.flow.StandardFlowSnapshotContext;
import org.apache.nifi.registry.provider.generated.Property;
import org.apache.nifi.registry.provider.generated.Provider;
import org.apache.nifi.registry.provider.generated.Providers;
import org.apache.nifi.registry.serialization.VersionedProcessGroupSerializer;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.OkResult;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ExportFlowSnapshots extends AbstractNiFiRegistryCommand<OkResult> {

    public ExportFlowSnapshots() {
        super("export-flow-snapshots", OkResult.class);
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public String getDescription() {
        // TODO: Additional detailed migration guid document is needed.
        return String.format("Export all flow snapshots and persist them" +
                        " with a FlowPersistenceProvider configured in the XML file specified by %s." +
                        " This command can be used to migrate existing flow content files between different FlowPersistenceProviders.",
                CommandOption.INPUT_SOURCE.getLongName());
    }

    @Override
    public OkResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {

        final String inputSource = properties.getProperty(CommandOption.INPUT_SOURCE.getLongName());
        if (StringUtils.isBlank(inputSource)) {
            throw new MissingOptionException("Missing required option --" + CommandOption.INPUT_SOURCE.getLongName());
        }

        final File providersFile = new File(inputSource);
        if (!providersFile.isFile()) {
            throw new FileNotFoundException(inputSource + " was not found");
        }

        // TODO: Add dataModelVersion support so that flows can be exported with old version formats?

        final VersionedProcessGroupSerializer serializer = new VersionedProcessGroupSerializer();
        final FlowPersistenceProvider flowPersistenceProvider = getFlowPersistenceProvider(getProviders(providersFile));

        // Get All buckets
        final List<Bucket> buckets = client.getBucketClient().getAll();
        for (Bucket bucket : buckets) {

            final FlowClient flowClient = client.getFlowClient();
            final String bucketId = bucket.getIdentifier();

            // Get All flowIds
            final List<VersionedFlow> flows = flowClient.getByBucket(bucketId);
            for (VersionedFlow flow : flows) {

                final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();
                final String flowId = flow.getIdentifier();

                // Get All flow versions
                final List<VersionedFlowSnapshotMetadata> snapshotMetadataList
                        = snapshotClient.getSnapshotMetadata(bucketId, flowId);

                for (VersionedFlowSnapshotMetadata snapshotMetadata : snapshotMetadataList) {
                    final StandardFlowSnapshotContext snapshotContext
                            = new StandardFlowSnapshotContext.Builder(bucket, flow, snapshotMetadata).build();

                    // Serialize flow contents.
                    final VersionedFlowSnapshot flowSnapshot = snapshotClient.get(bucketId, flowId, snapshotMetadata.getVersion());
                    final VersionedProcessGroup flowContents = flowSnapshot.getFlowContents();
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    serializer.serialize(flowContents, out);

                    // Persist flow contents.
                    flowPersistenceProvider.saveFlowContent(snapshotContext, out.toByteArray());
                }
            }
        }

        return new OkResult(getContext().isInteractive());
    }

    private static final String PROVIDERS_XSD = "/providers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.registry.provider.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();
    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, StandardProviderFactory.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.", e);
        }
    }

    private Providers getProviders(final File providersConfigFile) {
        if (providersConfigFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(StandardProviderFactory.class.getResource(PROVIDERS_XSD));

                // attempt to unmarshal
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);

                // set the holder for later use
                final JAXBElement<Providers> element = unmarshaller.unmarshal(new StreamSource(providersConfigFile), Providers.class);
                return element.getValue();
            } catch (SAXException | JAXBException e) {
                throw new ProviderFactoryException("Unable to load the providers configuration file at: " + providersConfigFile.getAbsolutePath(), e);
            }
        } else {
            throw new ProviderFactoryException("Unable to find the providers configuration file at " + providersConfigFile.getAbsolutePath());
        }
    }

    private FlowPersistenceProvider getFlowPersistenceProvider(final Providers providers) {
        final Provider jaxbFlowProvider = providers.getFlowPersistenceProvider();
        final String flowProviderClassName = jaxbFlowProvider.getClazz();

        try {
            final Class<?> rawFlowProviderClass = Class.forName(flowProviderClassName);
            final Class<? extends FlowPersistenceProvider> flowProviderClass = rawFlowProviderClass.asSubclass(FlowPersistenceProvider.class);

            final Constructor constructor = flowProviderClass.getConstructor();
            final FlowPersistenceProvider flowPersistenceProvider = (FlowPersistenceProvider) constructor.newInstance();

            final ProviderConfigurationContext configurationContext = createConfigurationContext(jaxbFlowProvider.getProperty());
            flowPersistenceProvider.onConfigured(configurationContext);

            return flowPersistenceProvider;
        } catch (Exception e) {
            throw new ProviderFactoryException("Error creating FlowPersistenceProvider with class name: " + flowProviderClassName, e);
        }
    }

    private ProviderConfigurationContext createConfigurationContext(final List<Property> configProperties) {
        final Map<String,String> properties = new HashMap<>();

        if (configProperties != null) {
            configProperties.forEach(p -> properties.put(p.getName(), p.getValue()));
        }

        return new StandardProviderConfigurationContext(properties);
    }
}
