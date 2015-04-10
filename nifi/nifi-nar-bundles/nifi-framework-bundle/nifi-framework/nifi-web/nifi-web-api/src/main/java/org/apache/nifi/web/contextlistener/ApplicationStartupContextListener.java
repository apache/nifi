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

import java.io.IOException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
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

/**
 * Application context listener for starting the application. If the application
 * is configured for a standalone environment or the application is a node in a
 * clustered environment then a flow controller is created and managed.
 * Otherwise, we assume the application is running as the cluster manager in a
 * clustered environment. In this case, the cluster manager is created and
 * managed.
 *
 * @author unattributed
 */
public class ApplicationStartupContextListener implements ServletContextListener {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationStartupContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {

        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
        NiFiProperties properties = ctx.getBean("nifiProperties", NiFiProperties.class);
        if (properties.isClusterManager()) {
            try {

                logger.info("Starting Cluster Manager...");

                WebClusterManager clusterManager = ctx.getBean("clusterManager", WebClusterManager.class);
                clusterManager.start();

                logger.info("Cluster Manager started successfully.");
            } catch (final BeansException | IOException e) {
                throw new NiFiCoreException("Unable to start Cluster Manager.", e);
            }

        } else {
            FlowService flowService = null;
            try {
                flowService = ctx.getBean("flowService", FlowService.class);

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
                    final FlowController flowController = flowService.getController();
                    flowController.onFlowInitialized(properties.getAutoResumeState());

                    logger.info("Flow Controller started successfully.");
                }
            } catch (BeansException | RepositoryPurgeException | IOException e) {
                // ensure the flow service is terminated
                if (flowService != null && flowService.isRunning()) {
                    flowService.stop(false);
                }
                throw new NiFiCoreException("Unable to start Flow Controller.", e);
            }
        }

    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
        NiFiProperties properties = ctx.getBean("nifiProperties", NiFiProperties.class);
        if (properties.isClusterManager()) {
            try {
                logger.info("Initiating shutdown of Cluster Manager...");

                WebClusterManager clusterManager = ctx.getBean("clusterManager", WebClusterManager.class);
                clusterManager.stop();

                logger.info("Cluster Manager termination completed.");
            } catch (final BeansException | IOException e) {
                String msg = "Problem occured ensuring Cluster Manager was properly terminated due to " + e;
                if (logger.isDebugEnabled()) {
                    logger.warn(msg, e);
                } else {
                    logger.warn(msg);
                }
            }
        } else {
            try {

                logger.info("Initiating shutdown of flow service...");

                FlowService flowService = ctx.getBean("flowService", FlowService.class);
                if (flowService.isRunning()) {
                    flowService.stop(/**
                             * force
                             */
                            false);
                }

                logger.info("Flow service termination completed.");

            } catch (final Exception e) {
                String msg = "Problem occurred ensuring flow controller or repository was properly terminated due to " + e;
                if (logger.isDebugEnabled()) {
                    logger.warn(msg, e);
                } else {
                    logger.warn(msg);
                }
            }
        }
    }
}
