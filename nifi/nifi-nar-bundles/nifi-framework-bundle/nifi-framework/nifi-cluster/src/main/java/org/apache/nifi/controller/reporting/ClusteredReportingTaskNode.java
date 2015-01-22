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
package org.apache.nifi.controller.reporting;

import org.apache.nifi.cluster.manager.impl.ClusteredReportingContext;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;

public class ClusteredReportingTaskNode extends AbstractReportingTaskNode {

    private final EventAccess eventAccess;
    private final BulletinRepository bulletinRepository;
    private final ControllerServiceProvider serviceProvider;

    public ClusteredReportingTaskNode(final ReportingTask reportingTask, final String id, final ProcessScheduler scheduler,
            final EventAccess eventAccess, final BulletinRepository bulletinRepository, final ControllerServiceProvider serviceProvider,
            final ValidationContextFactory validationContextFactory) {
        super(reportingTask, id, serviceProvider, scheduler, validationContextFactory);

        this.eventAccess = eventAccess;
        this.bulletinRepository = bulletinRepository;
        this.serviceProvider = serviceProvider;
    }

    @Override
    public ReportingContext getReportingContext() {
        return new ClusteredReportingContext(eventAccess, bulletinRepository, getProperties(), serviceProvider);
    }

}
