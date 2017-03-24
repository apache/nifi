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
package org.apache.nifi.web.contextlistener;

import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.RepositoryPurgeException;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiCoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

/**
 * Application context listener for starting the application. If the application is configured for a standalone environment or the application is a node in a clustered environment then a flow
 * controller is created and managed. Otherwise, we assume the application is running as the cluster manager in a clustered environment. In this case, the cluster manager is created and managed.
 *
 */
public class ApplicationStartupContextListener implements ServletContextListener {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationStartupContextListener.class);

    private FlowController flowController = null;
    private FlowService flowService = null;
    private RequestReplicator requestReplicator = null;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        final ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
        final NiFiProperties properties = ctx.getBean("nifiProperties", NiFiProperties.class);
        try {
            flowService = ctx.getBean("flowService", FlowService.class);
            flowController = ctx.getBean("flowController", FlowController.class);
            requestReplicator = ctx.getBean("requestReplicator", RequestReplicator.class);

            // start and load the flow if we're not clustered (clustered flow loading should
            // happen once the application (wars) is fully loaded and initialized). non clustered
            // nifi instance need to load the flow before the application (wars) are fully loaded.
            // during the flow loading (below) the flow controller is lazily initialized. when the
            // flow is loaded after the application is completely initialized (wars deploy), as
            // required with a clustered node, users are able to make web requests before the flow
            // is loaded (and the flow controller is initialized) which shouldn't be allowed. moving
            // the flow loading here when not clustered resolves this.
            if (!properties.isNode()) {
                logger.info("Starting Flow Controller...");

                // start and load the flow
                flowService.start();
                flowService.load(null);

                /*
                 * Start up all processors if specified.
                 *
                 * When operating as the node, the cluster manager controls
                 * which processors should start. As part of the flow
                 * reloading actions, the node will start the necessary
                 * processors.
                 */
                flowController.onFlowInitialized(properties.getAutoResumeState());

                logger.info("Flow Controller started successfully.");
            }
        } catch (BeansException | RepositoryPurgeException | IOException e) {
            shutdown();
            throw new NiFiCoreException("Unable to start Flow Controller.", e);
        }

        try {
            // attempt to get a few beans that we want to to ensure properly created since they are lazily initialized
            ctx.getBean("loginIdentityProvider", LoginIdentityProvider.class);
            ctx.getBean("authorizer", Authorizer.class);
        } catch (final BeansException e) {
            shutdown();
            throw new NiFiCoreException("Unable to start Flow Controller.", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Initiating shutdown of flow service...");
        shutdown();
        logger.info("Flow service termination completed.");
    }

    private void shutdown() {
        try {
            // ensure the flow service is terminated
            if (flowService != null && flowService.isRunning()) {
                flowService.stop(false);
            }
        } catch (final Exception e) {
            final String msg = "Problem occurred ensuring flow controller or repository was properly terminated due to " + e;
            if (logger.isDebugEnabled()) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
        }

        try {
            // ensure the request replicator is shutdown
            if (requestReplicator != null) {
                requestReplicator.shutdown();
            }
        } catch (final Exception e) {
            final String msg = "Problem occurred ensuring request replicator was properly terminated due to " + e;
            if (logger.isDebugEnabled()) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
        }
    }
}
