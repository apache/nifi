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
package org.apache.nifi.events;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;

public final class BulletinFactory {

    private static final AtomicLong currentId = new AtomicLong(0);

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message) {
        final ComponentType type;
        switch (connectable.getConnectableType()) {
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT:
                type = ComponentType.REMOTE_PROCESS_GROUP;
                break;
            case INPUT_PORT:
                type = ComponentType.INPUT_PORT;
                break;
            case OUTPUT_PORT:
                type = ComponentType.OUTPUT_PORT;
                break;
            case PROCESSOR:
            default:
                type = ComponentType.PROCESSOR;
                break;
        }

        final ProcessGroup group = connectable.getProcessGroup();
        final String groupId = group == null ? null : group.getIdentifier();
        final String groupName = group == null ? null : group.getName();
        return BulletinFactory.createBulletin(groupId, groupName, connectable.getIdentifier(), type, connectable.getName(), category, severity, message);
    }

    public static Bulletin createBulletin(final String groupId, final String sourceId, final ComponentType sourceType, final String sourceName,
        final String category, final String severity, final String message) {
        final Bulletin bulletin = new ComponentBulletin(currentId.getAndIncrement());
        bulletin.setGroupId(groupId);
        bulletin.setSourceId(sourceId);
        bulletin.setSourceType(sourceType);
        bulletin.setSourceName(sourceName);
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        return bulletin;
    }

    public static Bulletin createBulletin(final String groupId, final String groupName, final String sourceId, final ComponentType sourceType,
            final String sourceName, final String category, final String severity, final String message) {
        final Bulletin bulletin = new ComponentBulletin(currentId.getAndIncrement());
        bulletin.setGroupId(groupId);
        bulletin.setGroupName(groupName);
        bulletin.setSourceId(sourceId);
        bulletin.setSourceType(sourceType);
        bulletin.setSourceName(sourceName);
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        return bulletin;
    }

    public static Bulletin createBulletin(final String category, final String severity, final String message) {
        final Bulletin bulletin = new SystemBulletin(currentId.getAndIncrement());
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        bulletin.setSourceType(ComponentType.FLOW_CONTROLLER);
        return bulletin;
    }
}
