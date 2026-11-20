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
package org.apache.nifi.groups;

import org.apache.nifi.connectable.ConnectableType;

import java.util.List;

/**
 * A disabled, or stopped and invalid, component that has FlowFiles queued and so keeps a {@link ProcessGroupDrain} from finishing.
 *
 * @param groupId the identifier of the Process Group that contains the component, or that contains the Remote Process Group of a Remote Process Group port
 * @param remoteProcessGroupId the identifier of the Remote Process Group of a Remote Process Group port, or {@code null} for any other component
 */
public record DrainBlockingComponent(String componentId, String name, ConnectableType componentType, String groupId, String remoteProcessGroupId, Reason reason,
                                     List<String> validationErrors, int queuedCount) {

    public enum Reason {
        INVALID,
        DISABLED
    }
}
