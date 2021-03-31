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

public final class BulletinFactory {

    public static Bulletin createSystemBulletin(final Connectable connectable, final String category, final String severity, final String message) {
        final ComponentType type = getSourceType(connectable);
        final ProcessGroup group = connectable.getProcessGroup();
        final String groupId = connectable.getProcessGroupIdentifier();
        final String groupName = group == null ? null : group.getName();
        final String groupPath = buildGroupPath(group);

        return new Bulletin.Builder()
                .setGroupId(groupId)
                .setGroupName(groupName)
                .setSourceId(connectable.getIdentifier())
                .setSourceType(type)
                .setSourceName(connectable.getName())
                .setCategory(category)
                .setLevel(severity)
                .setMessage(message)
                .setGroupPath(groupPath)
                .createBulletin();
    }

    private static String buildGroupPath(ProcessGroup group) {
        if (group == null) {
            return null;
        } else {
            StringBuilder path = new StringBuilder(group.getName());
            ProcessGroup parent = group.getParent();
            while (parent != null) {
                path.insert(0, parent.getName() + " / ");
                parent = parent.getParent();
            }
            return path.toString();
        }
    }

    public static Bulletin createSystemBulletin(final String category, final String severity, final String message) {
        return new Bulletin.Builder()
                .setSourceType(ComponentType.FLOW_CONTROLLER)
                .setCategory(category)
                .setLevel(severity)
                .setMessage(message)
                .createBulletin();
    }

    public static ComponentType getSourceType(Connectable connectable) {
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
        return type;
    }

    private BulletinFactory() {
    }
}
