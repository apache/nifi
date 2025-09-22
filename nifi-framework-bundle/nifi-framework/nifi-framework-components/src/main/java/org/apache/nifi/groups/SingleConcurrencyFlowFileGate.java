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

import org.apache.nifi.connectable.Port;

import java.util.concurrent.atomic.AtomicBoolean;

public class SingleConcurrencyFlowFileGate implements FlowFileGate {
    private final AtomicBoolean claimed = new AtomicBoolean(false);

    @Override
    public boolean tryClaim(final Port port) {
        // Check if the claim is already held and atomically set it to being held.
        final boolean alreadyClaimed = claimed.getAndSet(true);
        if (alreadyClaimed) {
            // If claim was already held, then this thread failed to obtain the claim.
            return false;
        }

        // We need to try to open flow into the Port's group. To do this, we need to get the data valve for the parent group,
        // as it is responsible for data flowing into and out of its children.
        final ProcessGroup dataValveGroup = port.getProcessGroup().getParent();
        final DataValve dataValve = dataValveGroup.getDataValve();
        final boolean openFlowIntoGroup = dataValve.tryOpenFlowIntoGroup(port.getProcessGroup());
        if (!openFlowIntoGroup) {
            claimed.set(false);
            return false;
        }

        // The claim is now held by this thread. Check if the ProcessGroup is empty.
        final boolean empty = !port.getProcessGroup().isDataQueued();
        if (empty) {
            // Process Group is empty so return true indicating that the claim is now held.
            return true;
        }

        // We have already opened flow into group, so now we must close it, since we are not allowing flow in
        dataValve.closeFlowIntoGroup(port.getProcessGroup());

        // Process Group was not empty, so we cannot allow any more FlowFiles through. Reset claimed to false and return false,
        // indicating that the caller did not obtain the claim.
        claimed.set(false);
        return false;
    }

    @Override
    public void releaseClaim(final Port port) {
        claimed.set(false);

        final ProcessGroup dataValveGroup = port.getProcessGroup().getParent();
        final DataValve dataValve = dataValveGroup.getDataValve();
        dataValve.closeFlowIntoGroup(port.getProcessGroup());
    }
}
