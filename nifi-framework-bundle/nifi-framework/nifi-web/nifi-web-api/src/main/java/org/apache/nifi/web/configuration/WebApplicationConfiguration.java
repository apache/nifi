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
package org.apache.nifi.web.configuration;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.audit.NiFiAuditor;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.StandardAuthorizableLookup;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardReloadComponent;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.NiFiServiceFacadeLock;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.StandardNiFiContentAccess;
import org.apache.nifi.web.StandardNiFiServiceFacade;
import org.apache.nifi.web.StandardNiFiWebConfigurationContext;
import org.apache.nifi.web.api.ApplicationResource;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsCollector;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsResultConverter;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsService;
import org.apache.nifi.web.api.metrics.jmx.StandardJmxMetricsService;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.controller.ControllerSearchService;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.impl.ComponentDAO;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.search.query.RegexSearchQueryParser;
import org.apache.nifi.web.util.ClusterReplicationComponentLifecycle;
import org.apache.nifi.web.util.LocalComponentLifecycle;
import org.apache.nifi.web.util.ParameterContextNameCollisionResolver;
import org.apache.nifi.web.util.ParameterContextReplacer;
import org.apache.nifi.web.util.SnippetUtils;
import org.apache.nifi.web.util.VirtualThreadParallelProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;

@ComponentScan(basePackageClasses = {
        ComponentDAO.class,
        NiFiAuditor.class,
        ApplicationResource.class
})
@Import({
        StandardAuthorizableLookup.class,
        StandardNiFiServiceFacade.class
})
@EnableAspectJAutoProxy(proxyTargetClass = true)
@Configuration
public class WebApplicationConfiguration {

    private final Authorizer authorizer;

    private final AccessPolicyDAO accessPolicyDao;

    private final AuditService auditService;

    private final BulletinRepository bulletinRepository;

    private final ExtensionManager extensionManager;

    private final FlowController flowController;

    private final FlowService flowService;

    private final NiFiProperties properties;

    private final RevisionManager revisionManager;

    private final RuntimeManifestService runtimeManifestService;

    private final ControllerSearchService controllerSearchService;

    private ClusterCoordinator clusterCoordinator;

    private RequestReplicator requestReplicator;

    private NiFiServiceFacade serviceFacade;

    public WebApplicationConfiguration(
            final Authorizer authorizer,
            final AccessPolicyDAO accessPolicyDao,
            final AuditService auditService,
            final BulletinRepository bulletinRepository,
            final ExtensionManager extensionManager,
            final FlowController flowController,
            final FlowService flowService,
            final NiFiProperties properties,
            final RevisionManager revisionManager,
            final RuntimeManifestService runtimeManifestService,
            final ControllerSearchService controllerSearchService
    ) {
        this.authorizer = authorizer;
        this.accessPolicyDao = accessPolicyDao;
        this.auditService = auditService;
        this.bulletinRepository = bulletinRepository;
        this.extensionManager = extensionManager;
        this.flowController = flowController;
        this.flowService = flowService;
        this.properties = properties;
        this.revisionManager = revisionManager;
        this.runtimeManifestService = runtimeManifestService;
        this.controllerSearchService = controllerSearchService;
    }

    @Autowired(required = false)
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @Autowired(required = false)
    public void setRequestReplicator(final RequestReplicator requestReplicator) {
        this.requestReplicator = requestReplicator;
    }

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Bean
    public EntityFactory entityFactory() {
        return new EntityFactory();
    }

    @Bean
    public ContentAccess contentAccess() {
        final StandardNiFiContentAccess contentAccess = new StandardNiFiContentAccess();
        contentAccess.setClusterCoordinator(clusterCoordinator);
        contentAccess.setProperties(properties);
        contentAccess.setRequestReplicator(requestReplicator);
        contentAccess.setServiceFacade(serviceFacade);
        return contentAccess;
    }

    @Bean
    public DtoFactory dtoFactory() {
        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setAuthorizer(authorizer);
        dtoFactory.setBulletinRepository(bulletinRepository);
        dtoFactory.setControllerServiceProvider(flowController.getControllerServiceProvider());
        dtoFactory.setEntityFactory(entityFactory());
        dtoFactory.setExtensionManager(extensionManager);
        return dtoFactory;
    }

    @Bean
    public ControllerServiceProvider controllerServiceProvider() {
        return flowController.getControllerServiceProvider();
    }

    @Bean
    public SnippetUtils snippetUtils() {
        final SnippetUtils snippetUtils = new SnippetUtils();
        snippetUtils.setDtoFactory(dtoFactory());
        snippetUtils.setFlowController(flowController);
        snippetUtils.setAccessPolicyDAO(accessPolicyDao);
        return snippetUtils;
    }

    @Bean
    public NiFiServiceFacadeLock serviceFacadeLock() {
        return new NiFiServiceFacadeLock();
    }

    @Bean
    public ClusterReplicationComponentLifecycle clusterComponentLifecycle() {
        final ClusterReplicationComponentLifecycle lifecycle = new ClusterReplicationComponentLifecycle();
        lifecycle.setClusterCoordinator(clusterCoordinator);
        lifecycle.setDtoFactory(dtoFactory());
        lifecycle.setRequestReplicator(requestReplicator);
        lifecycle.setServiceFacade(serviceFacade);
        return lifecycle;
    }

    @Bean
    public LocalComponentLifecycle localComponentLifecycle() {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setDtoFactory(dtoFactory());
        lifecycle.setServiceFacade(serviceFacade);
        lifecycle.setRevisionManager(revisionManager);
        return lifecycle;
    }

    @Bean
    public VirtualThreadParallelProcessingService parallelProcessingService() {
        return new VirtualThreadParallelProcessingService(properties);
    }

    @Bean
    public JmxMetricsService jmxMetricsService() {
        final JmxMetricsResultConverter metricsResultConverter = new JmxMetricsResultConverter();
        final JmxMetricsCollector metricsCollector = new JmxMetricsCollector(metricsResultConverter);
        final StandardJmxMetricsService jmxMetricsService = new StandardJmxMetricsService();
        jmxMetricsService.setMetricsCollector(metricsCollector);
        jmxMetricsService.setProperties(properties);
        return jmxMetricsService;
    }

    @Bean
    public ParameterContextReplacer parameterContextReplacer() {
        final ParameterContextNameCollisionResolver nameCollisionResolver = new ParameterContextNameCollisionResolver();
        return new ParameterContextReplacer(nameCollisionResolver);
    }

    @Bean
    public RegexSearchQueryParser searchQueryParser() {
        return new RegexSearchQueryParser();
    }

    @Bean
    public StandardReloadComponent reloadComponent() {
        return new StandardReloadComponent(flowController);
    }

    @Bean
    public NiFiWebConfigurationContext nifiWebConfigurationContext() {
        final StandardNiFiWebConfigurationContext context = new StandardNiFiWebConfigurationContext();
        context.setAuthorizer(authorizer);
        context.setAuditService(auditService);
        context.setProperties(properties);
        context.setClusterCoordinator(clusterCoordinator);
        context.setControllerServiceProvider(flowController.getControllerServiceProvider());
        context.setRequestReplicator(requestReplicator);
        context.setReportingTaskProvider(flowController);
        context.setServiceFacade(serviceFacade);
        return context;
    }

    @Bean
    public ControllerFacade controllerFacade() {
        final ControllerFacade controllerFacade = new ControllerFacade();
        controllerFacade.setAuthorizer(authorizer);
        controllerFacade.setFlowController(flowController);
        controllerFacade.setDtoFactory(dtoFactory());
        controllerFacade.setProperties(properties);
        controllerFacade.setFlowService(flowService);
        controllerFacade.setRuntimeManifestService(runtimeManifestService);
        controllerFacade.setControllerSearchService(controllerSearchService);
        controllerFacade.setSearchQueryParser(searchQueryParser());
        return controllerFacade;
    }
}
