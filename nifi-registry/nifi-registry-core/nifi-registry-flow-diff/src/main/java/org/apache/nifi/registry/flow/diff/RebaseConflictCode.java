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

package org.apache.nifi.registry.flow.diff;

/**
 * Identifies the reason a local change could not be classified as compatible during a rebase analysis.
 */
public enum RebaseConflictCode {

    /**
     * No RebaseHandler is registered for the local change's difference type.
     */
    NO_HANDLER,

    /**
     * The local change does not specify the field name required to classify it.
     */
    MISSING_FIELD_NAME,

    /**
     * Both the local and upstream flows modified the same property on the same component.
     */
    SAME_PROPERTY,

    /**
     * Both the local and upstream flows modified the comments on the same component.
     */
    SAME_COMPONENT_COMMENTS,

    /**
     * The component targeted by the local change does not exist in the target version.
     */
    COMPONENT_NOT_FOUND,

    /**
     * The property descriptor targeted by the local change changed in an incompatible way in the target version.
     */
    DESCRIPTOR_CHANGED,

    /**
     * The property descriptor targeted by the local change does not exist in the target version.
     */
    DESCRIPTOR_NOT_FOUND
}
