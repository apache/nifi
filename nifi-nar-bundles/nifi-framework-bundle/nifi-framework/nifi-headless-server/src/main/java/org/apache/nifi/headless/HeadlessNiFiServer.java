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
package org.apache.nifi.headless;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.impl.StandardAuditService;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowService;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.diagnostics.ThreadDumpTask;
import org.apache.nifi.diagnostics.bootstrap.BootstrapDiagnosticsFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorFactory;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.StandardFlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.FlowParser;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Set;

/**
 */
public class HeadlessNiFiServer implements NiFiServer {

    private static final Logger logger = LoggerFactory.getLogger(HeadlessNiFiServer.class);
    protected NiFiProperties props;
    protected Bundle systemBundle;
    protected Set<Bundle> bundles;
    protected FlowController flowController;
    protected FlowService flowService;
    protected DiagnosticsFactory diagnosticsFactory;

    /**
     * Default constructor
     */
    public HeadlessNiFiServer() {
    }

    public void start() {
        try {

            // Create a standard extension manager and discover extensions
            final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
            extensionManager.discoverExtensions(systemBundle, bundles);
            extensionManager.logClassLoaderMapping();

            // Set the extension manager into the holder which makes it available to the Spring context via a factory bean
            ExtensionManagerHolder.init(extensionManager);

            // Enrich the flow xml using the Extension Manager mapping
            final FlowParser flowParser = new FlowParser();
            final FlowEnricher flowEnricher = new FlowEnricher(this, flowParser, props);
            flowEnricher.enrichFlowWithBundleInformation();
            logger.info("Loading Flow...");

            FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
            AuditService auditService = new StandardAuditService();
            Authorizer authorizer = new Authorizer() {
                @Override
                public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
                    return AuthorizationResult.approved();
                }

                @Override
                public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
                    // do nothing
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    // do nothing
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    // do nothing
                }
            };

            PropertyEncryptor encryptor = PropertyEncryptorFactory.getPropertyEncryptor(props);
            VariableRegistry variableRegistry = new FileBasedVariableRegistry(props.getVariableRegistryPropertiesPaths());
            BulletinRepository bulletinRepository = new VolatileBulletinRepository();
            StandardFlowRegistryClient flowRegistryClient = new StandardFlowRegistryClient();
            flowRegistryClient.setProperties(props);

            flowController = FlowController.createStandaloneInstance(
                    flowFileEventRepository,
                    props,
                    authorizer,
                    auditService,
                    encryptor,
                    bulletinRepository,
                    variableRegistry,
                    flowRegistryClient,
                    extensionManager);

            flowService = StandardFlowService.createStandaloneInstance(
                    flowController,
                    props,
                    encryptor,
                    null, // revision manager
                    authorizer);

            diagnosticsFactory = new BootstrapDiagnosticsFactory();
            ((BootstrapDiagnosticsFactory) diagnosticsFactory).setFlowController(flowController);
            ((BootstrapDiagnosticsFactory) diagnosticsFactory).setNifiProperties(props);

            // start and load the flow
            flowService.start();
            flowService.load(null);
            flowController.onFlowInitialized(true);
            FlowManager flowManager = flowController.getFlowManager();
            flowManager.getGroup(flowManager.getRootGroupId()).startProcessing();

            logger.info("Flow loaded successfully.");
        } catch (Exception e) {
            // ensure the flow service is terminated
            if (flowService != null && flowService.isRunning()) {
                flowService.stop(false);
            }
            startUpFailure(new Exception("Unable to load flow due to: " + e, e));
        }
    }

    private void startUpFailure(Throwable t) {
        System.err.println("Failed to start flow service: " + t.getMessage());
        System.err.println("Shutting down...");
        logger.warn("Failed to start headless server... shutting down.", t);
        System.exit(1);
    }

    @Override
    public void initialize(NiFiProperties properties, Bundle systemBundle, Set<Bundle> bundles, ExtensionMapping extensionMapping) {
        this.props = properties;
        this.systemBundle = systemBundle;
        this.bundles = bundles;
    }

    @Override
    public DiagnosticsFactory getDiagnosticsFactory() {
        // The diagnosticsFactory is initialized during server startup. If the diagnostics factory happens to be
        // requested before the Server starts, or after the server fails to start, we cannot provide the fully initialized
        // diagnostics factory. But it is still helpful to provide what we can, so we will provide the Thread Dump Factory.
        return diagnosticsFactory == null ? getThreadDumpFactory() : diagnosticsFactory;
    }

    @Override
    public DiagnosticsFactory getThreadDumpFactory() {
        return new ThreadDumpDiagnosticsFactory();
    }

    @Override
    public DecommissionTask getDecommissionTask() {
        return null;
    }

    public void stop() {
        try {
            flowService.stop(false);
        } catch (Exception e) {
            String msg = "Problem occurred ensuring flow controller or repository was properly terminated due to " + e;
            if (logger.isDebugEnabled()) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
        }
    }

    protected List<Bundle> getBundles(final String bundleClass) {
        return ExtensionManagerHolder.getExtensionManager().getBundles(bundleClass);
    }

    private static class ThreadDumpDiagnosticsFactory implements DiagnosticsFactory {
        @Override
        public DiagnosticsDump create(final boolean verbose) {
            return new DiagnosticsDump() {
                @Override
                public void writeTo(final OutputStream out) throws IOException {
                    final DiagnosticsDumpElement threadDumpElement = new ThreadDumpTask().captureDump(verbose);
                    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
                    for (final String detail : threadDumpElement.getDetails()) {
                        writer.write(detail);
                        writer.write("\n");
                    }

                    writer.flush();
                }
            };
        }
    }
}
