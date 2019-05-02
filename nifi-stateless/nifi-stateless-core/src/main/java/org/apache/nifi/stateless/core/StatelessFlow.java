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
package org.apache.nifi.stateless.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.stateless.bootstrap.ExtensionDiscovery;
import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.stateless.bootstrap.RunnableFlow;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class StatelessFlow implements RunnableFlow {

    public static final String REGISTRY = "registryUrl";
    public static final String BUCKETID = "bucketId";
    public static final String FLOWID = "flowId";
    public static final String FLOWVERSION = "flowVersion";
    public static final String MATERIALIZECONTENT = "materializeContent";
    public static final String FAILUREPORTS = "failurePortIds";
    public static final String FLOWFILES = "flowFiles";
    public static final String CONTENT = "nifi_content";
    public static final String VARIABLES = "variables";

    public static final String SSL = "ssl";
    public static final String KEYSTORE = "keystore";
    public static final String KEYSTORE_PASS = "keystorePass";
    public static final String KEY_PASS = "keyPass";
    public static final String KEYSTORE_TYPE = "keystoreType";
    public static final String TRUSTSTORE = "truststore";
    public static final String TRUSTSTORE_PASS = "truststorePass";
    public static final String TRUSTSTORE_TYPE = "truststoreType";

    private static final String DEFAULT_WORKING_DIR = "./work";

    private List<StatelessComponent> roots;
    private volatile boolean stopRequested = false;
    private StatelessComponent sourceComponent = null;

    private final ComponentFactory componentFactory;

    public StatelessFlow(final StatelessProcessorWrapper root) {
        this(Collections.singletonList(root));
    }

    public StatelessFlow(final List<StatelessComponent> roots) {
        this.roots = roots;
        this.componentFactory = null;
    }


    public StatelessFlow(final VersionedProcessGroup flow, final ExtensionManager extensionManager, final VariableRegistry variableRegistry,
                         final List<String> failureOutputPorts, final boolean materializeContent, final SSLContext sslContext) throws ProcessorInstantiationException,  InitializationException {

        this.componentFactory = new ComponentFactory(extensionManager);

        final Map<String, VersionedProcessor> processors = findProcessorsRecursive(flow).stream()
            .collect(Collectors.toMap(VersionedProcessor::getIdentifier, proc -> proc));

        final Map<String, VersionedRemoteProcessGroup> rpgs = new HashMap<>();
        final Map<String, VersionedRemoteGroupPort> remotePorts = new HashMap<>();
        findRemoteGroupRecursive(flow, rpgs, remotePorts);

        final Set<VersionedConnection> connections = findConnectionsRecursive(flow);
        final Set<VersionedPort> inputPorts = flow.getInputPorts();

        if (inputPorts.size() > 1) {
            throw new IllegalArgumentException("Only one input port per flow is allowed");
        }

        final StatelessControllerServiceLookup serviceLookup = new StatelessControllerServiceLookup();

        final Set<VersionedControllerService> controllerServices = flow.getControllerServices();
        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerService service = componentFactory.createControllerService(versionedControllerService, variableRegistry);
            serviceLookup.addControllerService(service);
        }

        final Map<String, StatelessComponent> componentMap = new HashMap<>();

        for (final VersionedConnection connection : connections) {
            boolean isInputPortConnection = false;

            final ConnectableComponent source = connection.getSource();
            final ConnectableComponent destination = connection.getDestination();

            StatelessComponent sourceComponent = null;
            if (componentMap.containsKey(source.getId())) {
                sourceComponent = componentMap.get(source.getId());
            } else {
                switch (source.getType()) {
                    case PROCESSOR:
                        final VersionedProcessor processor = processors.get(source.getId());

                        if (processor == null) {
                            throw new IllegalArgumentException("Unknown input processor. " + source.getId());
                        } else {
                            sourceComponent = componentFactory.createProcessor(processor, materializeContent, serviceLookup, variableRegistry, null);
                            componentMap.put(source.getId(), sourceComponent);
                        }
                        break;
                    case REMOTE_INPUT_PORT:
                        throw new IllegalArgumentException("Unsupported source type: " + source.getType());
                    case REMOTE_OUTPUT_PORT:
                        final VersionedRemoteGroupPort remotePort = remotePorts.get(source.getId());
                        final VersionedRemoteProcessGroup rpg = rpgs.get(remotePort.getRemoteGroupId());

                        sourceComponent = new StatelessRemoteOutputPort(rpg, remotePort, sslContext);
                        componentMap.put(source.getId(), sourceComponent);
                        break;
                    case OUTPUT_PORT:
                    case FUNNEL:
                        sourceComponent = new StatelessPassThroughComponent();
                        componentMap.put(source.getId(), sourceComponent);
                        break;
                    case INPUT_PORT:
                        if (flow.getIdentifier().equals(connection.getGroupIdentifier())) {
                            isInputPortConnection = true;
                        } else {
                            sourceComponent = new StatelessPassThroughComponent();
                            componentMap.put(source.getId(), sourceComponent);
                        }

                        break;
                }
            }

            StatelessComponent destinationComponent = null;
            switch (destination.getType()) {
                case PROCESSOR:
                    if (componentMap.containsKey(destination.getId())) {
                        destinationComponent = componentMap.get(destination.getId());
                    } else {
                        final VersionedProcessor processor = processors.get(destination.getId());
                        if (processor == null) {
                            return;
                        }

                        destinationComponent = componentFactory.createProcessor(processor, materializeContent, serviceLookup, variableRegistry, null);
                        destinationComponent.addParent(sourceComponent);
                        componentMap.put(destination.getId(), destinationComponent);
                    }

                    break;
                case REMOTE_INPUT_PORT:
                    if (componentMap.containsKey(destination.getId())) {
                        destinationComponent = componentMap.get(destination.getId());
                    } else {
                        final VersionedRemoteGroupPort remotePort = remotePorts.get(destination.getId());
                        final VersionedRemoteProcessGroup rpg = rpgs.get(remotePort.getRemoteGroupId());

                        destinationComponent = new StatelessRemoteInputPort(rpg, remotePort, sslContext);
                        destinationComponent.addParent(sourceComponent);
                        componentMap.put(destination.getId(), destinationComponent);
                    }

                    break;
                case REMOTE_OUTPUT_PORT:
                    throw new IllegalArgumentException("Unsupported destination type: " + destination.getType());
                case OUTPUT_PORT:
                    if (isInputPortConnection) {
                        throw new IllegalArgumentException("Input ports can not be mapped directly to output ports...");
                    }

                    // If Output Port is top-level port, treat it differently than if it's an inner group.
                    if (flow.getIdentifier().equals(connection.getGroupIdentifier())) {
                        //Link source and destination
                        for (final String selectedRelationship : connection.getSelectedRelationships()) {
                            final Relationship relationship = new Relationship.Builder().name(selectedRelationship).build();
                            final boolean failurePort = failureOutputPorts.contains(destination.getId());
                            sourceComponent.addOutputPort(relationship, failurePort);
                        }

                        break;
                    }

                    // Intentionally let the flow drop-through, and treat the same as an output port or funnel.
                case INPUT_PORT:
                case FUNNEL:
                    if (componentMap.containsKey(destination.getId())) {
                        destinationComponent = componentMap.get(destination.getId());
                    } else {
                        destinationComponent = new StatelessPassThroughComponent();
                        componentMap.put(destination.getId(), destinationComponent);
                    }

                    break;
            }

            if (destinationComponent != null) {
                destinationComponent.addIncomingConnection(connection.getIdentifier());

                if (isInputPortConnection) {
                    this.sourceComponent = destinationComponent;
                } else {
                    destinationComponent.addParent(sourceComponent);

                    //Link source and destination
                    for (final String relationship : connection.getSelectedRelationships()) {
                        sourceComponent.addChild(destinationComponent, new Relationship.Builder().name(relationship).build());
                    }
                }

            }
        }

        roots = componentMap.entrySet()
            .stream()
            .filter(e -> e.getValue().getParents().isEmpty())
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }

    private Set<VersionedProcessor> findProcessorsRecursive(final VersionedProcessGroup group) {
        final Set<VersionedProcessor> processors = new HashSet<>();
        findProcessorsRecursive(group, processors);
        return processors;
    }

    private void findProcessorsRecursive(final VersionedProcessGroup group, final Set<VersionedProcessor> processors) {
        processors.addAll(group.getProcessors());
        group.getProcessGroups().forEach(child -> findProcessorsRecursive(child, processors));
    }

    private Set<VersionedConnection> findConnectionsRecursive(final VersionedProcessGroup group) {
        final Set<VersionedConnection> connections = new HashSet<>();
        findConnectionsRecursive(group, connections);
        return connections;
    }

    private void findConnectionsRecursive(final VersionedProcessGroup group, final Set<VersionedConnection> connections) {
        connections.addAll(group.getConnections());
        group.getProcessGroups().forEach(child -> findConnectionsRecursive(child, connections));
    }

    private void findRemoteGroupRecursive(final VersionedProcessGroup group, final Map<String, VersionedRemoteProcessGroup> rpgs, final Map<String, VersionedRemoteGroupPort> ports) {
        for (final VersionedRemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            rpgs.put(rpg.getIdentifier(), rpg);

            rpg.getInputPorts().forEach(port -> ports.put(port.getIdentifier(), port));
            rpg.getOutputPorts().forEach(port -> ports.put(port.getIdentifier(), port));
        }
    }



    public boolean run(final Queue<InMemoryFlowFile> output) {
        while (!this.stopRequested) {
            for (final StatelessComponent pw : roots) {
                final boolean successful = pw.runRecursive(output);
                if (!successful) {
                    return false;
                }
            }
        }

        return true;
    }

    public boolean runOnce(Queue<InMemoryFlowFile> output) {
        for (final StatelessComponent pw : roots) {
            final boolean successful = pw.runRecursive(output);
            if (!successful) {
                return false;
            }
        }

        return true;
    }

    public void shutdown() {
        this.stopRequested = true;
        this.roots.forEach(StatelessComponent::shutdown);
    }

    public static SSLContext getSSLContext(final JsonObject config) {
        if (!config.has(SSL)) {
            return null;
        }

        final JsonObject sslObject = config.get(SSL).getAsJsonObject();
        if (sslObject.has(KEYSTORE) && sslObject.has(KEYSTORE_PASS) && sslObject.has(KEYSTORE_TYPE)
                && sslObject.has(TRUSTSTORE) && sslObject.has(TRUSTSTORE_PASS) && sslObject.has(TRUSTSTORE_TYPE)) {

            final String keystore = sslObject.get(KEYSTORE).getAsString();
            final String keystorePass = sslObject.get(KEYSTORE_PASS).getAsString();
            final String keyPass = sslObject.has(KEY_PASS) ? sslObject.get(KEY_PASS).getAsString() : keystorePass;
            final String keystoreType = sslObject.get(KEYSTORE_TYPE).getAsString();

            final String truststore = sslObject.get(TRUSTSTORE).getAsString();
            final String truststorePass = sslObject.get(TRUSTSTORE_PASS).getAsString();
            final String truststoreType = sslObject.get(TRUSTSTORE_TYPE).getAsString();

            try {
                return SslContextFactory.createSslContext(keystore, keystorePass.toCharArray(), keyPass.toCharArray(), keystoreType,
                    truststore, truststorePass.toCharArray(), truststoreType, SslContextFactory.ClientAuth.REQUIRED, "TLS");
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create Keystore", e);
            }
        }

        return null;
    }

    public static StatelessFlow createAndEnqueueFromJSON(final JsonObject args, final ClassLoader systemClassLoader, final File narWorkingDir)
            throws InitializationException, IOException, ProcessorInstantiationException, NiFiRegistryException {
        if (args == null) {
            throw new IllegalArgumentException("Flow arguments can not be null");
        }

        System.out.println("Running flow from json: " + args.toString());

        if (!args.has(REGISTRY) || !args.has(BUCKETID) || !args.has(FLOWID)) {
            throw new IllegalArgumentException("The following parameters must be provided: " + REGISTRY + ", " + BUCKETID + ", " + FLOWID);
        }

        final String registryurl = args.getAsJsonPrimitive(REGISTRY).getAsString();
        final String bucketID = args.getAsJsonPrimitive(BUCKETID).getAsString();
        final String flowID = args.getAsJsonPrimitive(FLOWID).getAsString();

        int flowVersion = -1;
        if (args.has(FLOWVERSION)) {
            flowVersion = args.getAsJsonPrimitive(FLOWVERSION).getAsInt();
        }

        boolean materializeContent = true;
        if (args.has(MATERIALIZECONTENT)) {
            materializeContent = args.getAsJsonPrimitive(MATERIALIZECONTENT).getAsBoolean();
        }

        final List<String> failurePorts = new ArrayList<>();
        if (args.has(FAILUREPORTS)) {
            args.getAsJsonArray(FAILUREPORTS).forEach(port ->failurePorts.add(port.getAsString()));
        }

        final Map<VariableDescriptor, String> inputVariables = new HashMap<>();

        if (args.has(VARIABLES)) {
            final JsonElement variablesElement = args.get(VARIABLES);
            final JsonObject variablesObject = variablesElement.getAsJsonObject();
            variablesObject.entrySet()
                .forEach(entry ->inputVariables.put(new VariableDescriptor(entry.getKey()), entry.getValue().getAsString()));
        }

        final SSLContext sslContext = getSSLContext(args);

        final VersionedFlowSnapshot snapshot = new RegistryUtil(registryurl, sslContext).getFlowByID(bucketID, flowID, flowVersion);
        final ExtensionManager extensionManager = ExtensionDiscovery.discover(narWorkingDir, systemClassLoader);

        final StatelessFlow flow = new StatelessFlow(snapshot.getFlowContents(), extensionManager, () -> inputVariables, failurePorts, materializeContent, sslContext);
        flow.enqueueFromJSON(args);
        return flow;
    }

    public void enqueueFlowFile(final byte[] content, final Map<String, String> attributes) {
        if (sourceComponent == null) {
            throw new IllegalArgumentException("Flow does not have an input port...");
        }

        //enqueue data
        final Queue<StatelessFlowFile> input = new LinkedList<>();
        input.add(new StatelessFlowFile(content, attributes, sourceComponent.isMaterializeContent()));

        sourceComponent.enqueueAll(input);
    }

    public void enqueueFromJSON(final JsonObject json) {
        final JsonArray flowFiles;
        if (json.has(FLOWFILES)) {
            flowFiles = json.getAsJsonArray(FLOWFILES);
        } else {
            return;
        }

        if (flowFiles.size() == 0) {
            return;
        }

        if (sourceComponent == null) {
            throw new IllegalStateException("Configuration specifies to inject " + flowFiles.size() + " FlowFiles into the flow, but the Flow does not contain an Input Port.");
        }

        final Queue<StatelessFlowFile> input = new LinkedList<>();
        flowFiles.forEach(f -> {
            final JsonObject file = f.getAsJsonObject();
            final String content = file.getAsJsonPrimitive(CONTENT).getAsString();

            final Map<String, String> attributes = new HashMap<>();
            file.entrySet().forEach(entry -> {
                if (!CONTENT.equals(entry.getKey())) {
                    attributes.put(entry.getKey(), entry.getValue().getAsString());
                }
            });

            input.add(new StatelessFlowFile(content, attributes, sourceComponent.isMaterializeContent()));
        });

        //enqueue data
        sourceComponent.enqueueAll(input);
    }
}
