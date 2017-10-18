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
package org.apache.nifi.logging;

import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.Severity;

public class ControllerServiceLogObserver implements LogObserver {

    private final BulletinRepository bulletinRepository;
    private final ControllerServiceNode serviceNode;

    public ControllerServiceLogObserver(final BulletinRepository bulletinRepository, final ControllerServiceNode serviceNode) {
        this.bulletinRepository = bulletinRepository;
        this.serviceNode = serviceNode;
    }

    @Override
    public void onLogMessage(final LogMessage message) {
        // Map LogLevel.WARN to Severity.WARNING so that we are consistent with the Severity enumeration. Else, just use whatever
        // the LogLevel is (INFO and ERROR map directly and all others we will just accept as they are).
        final String bulletinLevel = message.getLevel() == LogLevel.WARN ? Severity.WARNING.name() : message.getLevel().toString();

        final ProcessGroup pg = serviceNode.getProcessGroup();
        final String groupId = pg == null ? null : pg.getIdentifier();
        final String groupName = pg == null ? null : pg.getName();

        final Bulletin bulletin = BulletinFactory.createBulletin(groupId, groupName, serviceNode.getIdentifier(), ComponentType.CONTROLLER_SERVICE,
                serviceNode.getName(), "Log Message", bulletinLevel, message.getMessage());
        bulletinRepository.addBulletin(bulletin);
    }
}
