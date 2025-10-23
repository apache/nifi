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

import java.io.PrintWriter;
import java.io.StringWriter;
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

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message, final String flowFileUUID, final Throwable t) {
        final Bulletin bulletin = createBulletin(connectable, category, severity, message, flowFileUUID);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
        return bulletin;
    }

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message, final Throwable t) {
        final Bulletin bulletin = createBulletin(connectable, category, severity, message);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
        return bulletin;
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

    public static Bulletin createBulletin(final String groupId, final String sourceId, final ComponentType sourceType, final String sourceName,
        final String category, final String severity, final String message, final Throwable t) {
        final Bulletin bulletin = createBulletin(groupId, sourceId, sourceType, sourceName, category, severity, message);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
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
            final String sourceName, final String category, final String severity, final String message, final Throwable t) {
        final Bulletin bulletin = createBulletin(groupId, groupName, sourceId, sourceType, sourceName, category, severity, message);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
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

    public static Bulletin createBulletin(final String groupId, final String groupName, final String sourceId, final ComponentType sourceType,
            final String sourceName, final String category, final String severity, final String message, final String groupPath, final String flowFileUUID, final Throwable t) {
        final Bulletin bulletin = createBulletin(groupId, groupName, sourceId, sourceType, sourceName, category, severity, message, groupPath, flowFileUUID);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
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

    public static Bulletin createBulletin(final String category, final String severity, final String message, final Throwable t) {
        final Bulletin bulletin = createBulletin(category, severity, message);
        if (t != null) {
            bulletin.setStackTrace(formatStackTrace(t));
        }
        return bulletin;
    }

    private static ComponentType getComponentType(final Connectable connectable) {
        return switch (connectable.getConnectableType()) {
            case REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT -> ComponentType.REMOTE_PROCESS_GROUP;
            case INPUT_PORT -> ComponentType.INPUT_PORT;
            case OUTPUT_PORT -> ComponentType.OUTPUT_PORT;
            case STATELESS_GROUP -> ComponentType.PROCESS_GROUP;
            default -> ComponentType.PROCESSOR;
        };
    }

    private static String formatStackTrace(final Throwable t) {
        try (final StringWriter sw = new StringWriter(); final PrintWriter pw = new PrintWriter(sw)) {
            t.printStackTrace(pw);
            pw.flush();
            return sw.toString();
        } catch (final Exception e) {
            // Fallback to Throwable#toString if printing fails for any reason
            return t.toString();
        }
    }
}
