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
package org.apache.nifi.minifi;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.admin.service.impl.StandardAuditService;
import org.apache.nifi.admin.service.impl.StandardUserService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowService;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RingBufferEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.minifi.status.StatusConfigReporter;
import org.apache.nifi.minifi.status.StatusRequestException;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class MiNiFiServer {

    private static final Logger logger = LoggerFactory.getLogger(MiNiFiServer.class);
    private final NiFiProperties props;
    private FlowService flowService;

    /**
     *
     * @param props the configuration
     */
    public MiNiFiServer(final NiFiProperties props) {
        this.props = props;
    }

    public void start() {
        try {
            logger.info("Loading Flow...");

            FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
            AuditService auditService = new StandardAuditService();
            UserService userService = new StandardUserService();
            StringEncryptor encryptor = StringEncryptor.createEncryptor();

            FlowController flowController = FlowController.createStandaloneInstance(
                    flowFileEventRepository,
                    props,
                    userService,
                    auditService,
                    encryptor);

            flowService = StandardFlowService.createStandaloneInstance(
                    flowController,
                    props,
                    encryptor);

            // start and load the flow
            flowService.start();
            flowService.load(null);
            flowController.onFlowInitialized(true);
            flowController.getGroup(flowController.getRootGroupId()).startProcessing();

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
        logger.warn("Failed to start minifi server... shutting down.", t);
        System.exit(1);
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

    public FlowStatusReport getStatusReport(String requestString) throws StatusRequestException {
        return StatusConfigReporter.getStatus(flowService.getController(), requestString, logger);
    }
}
