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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;

import java.util.concurrent.atomic.AtomicLong;

public final class BulletinFactory {

    private static final AtomicLong currentId = new AtomicLong(0);

    private BulletinFactory() {
    }

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message) {
        final ComponentType type = getComponentType(connectable);

        final ProcessGroup group = connectable.getProcessGroup();
        final String groupId = connectable.getProcessGroupIdentifier();
        final String groupName = group == null ? null : group.getName();
        final String groupPath = buildGroupPath(group);
        return createBulletin(groupId, groupName, connectable.getIdentifier(), type, connectable.getName(), category, severity, message, groupPath, null);
    }

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message, final String flowFileUUID) {
        final ComponentType type = getComponentType(connectable);

        final ProcessGroup group = connectable.getProcessGroup();
        final String groupId = connectable.getProcessGroupIdentifier();
        final String groupName = group == null ? null : group.getName();
        final String groupPath = buildGroupPath(group);
        return createBulletin(groupId, groupName, connectable.getIdentifier(), type, connectable.getName(), category, severity, message, groupPath, flowFileUUID);
    }

    private static String buildGroupPath(ProcessGroup group) {
        if (group == null) {
            return null;
        } else {
            StringBuilder path = new StringBuilder(group.getName());
            ProcessGroup parent = group.getParent();
            while (parent != null) {
                path.insert(0, " / ");
                path.insert(0, parent.getName());
                parent = parent.getParent();
            }
            return path.toString();
        }
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

    public static Bulletin createBulletin(final String groupId, final String groupName, final String sourceId, final ComponentType sourceType,
            final String sourceName, final String category, final String severity, final String message, final String groupPath, final String flowFileUUID) {
        final Bulletin bulletin = new ComponentBulletin(currentId.getAndIncrement());
        bulletin.setGroupId(groupId);
        bulletin.setGroupName(groupName);
        bulletin.setGroupPath(groupPath);
        bulletin.setSourceId(sourceId);
        bulletin.setSourceType(sourceType);
        bulletin.setSourceName(sourceName);
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        bulletin.setFlowFileUuid(flowFileUUID);
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

    private static ComponentType getComponentType(final Connectable connectable) {
        switch (connectable.getConnectableType()) {
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT:
                return ComponentType.REMOTE_PROCESS_GROUP;
            case INPUT_PORT:
                return ComponentType.INPUT_PORT;
            case OUTPUT_PORT:
                return ComponentType.OUTPUT_PORT;
            case STATELESS_GROUP:
                return ComponentType.PROCESS_GROUP;
            case PROCESSOR:
            default:
                return ComponentType.PROCESSOR;
        }
    }
}
