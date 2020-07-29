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
package org.apache.nifi.controller.inheritance;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;

public interface FlowInheritabilityCheck {
    /**
     * Determines whether or not the given proposed flow can be inherited, based on the given existing flow
     *
     * @param existingFlow the existing flow
     * @param proposedFlow the flow that is being proposed for inheritance
     * @param flowController the FlowController that can be used to understand additional context about the current state
     *
     * @return a FlowInheritability indicating whether or not the flow is inheritable
     */
    FlowInheritability checkInheritability(DataFlow existingFlow, DataFlow proposedFlow, FlowController flowController);
}
