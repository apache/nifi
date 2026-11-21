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

package org.apache.nifi.stateless.flow;

import java.util.Objects;
import java.util.Optional;

/**
 * The component that threw the Exception that caused a dataflow to fail.
 *
 * @param id          the identifier of the component within the running dataflow
 * @param versionedId the identifier of the component in the flow definition, if it has one
 * @param name        the name of the component
 * @param type        the fully qualified class name of a Processor, or the kind of component otherwise, such as "Funnel"
 * @param groupId     the identifier of the Process Group that contains the component, or <code>null</code> if it has none
 * @param groupName   the name of the Process Group that contains the component, or <code>null</code> if it has none
 */
public record FailingComponent(String id, Optional<String> versionedId, String name, String type, String groupId, String groupName) {

    public FailingComponent {
        Objects.requireNonNull(id, "Component ID required");
        Objects.requireNonNull(versionedId, "Versioned Component ID Optional required");
    }
}
