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

    public SingleConcurrencyFlowFileGate() {
    }

    @Override
    public boolean tryClaim(final Port port) {
        // Check if the claim is already held and atomically set it to being held.
        final boolean alreadyClaimed = claimed.getAndSet(true);
        if (alreadyClaimed) {
            // If claim was already held, then this thread failed to obtain the claim.
            return false;
        }

        // The claim is now held by this thread. Check if the ProcessGroup is empty.
        final boolean empty = !port.getProcessGroup().isDataQueued();
        if (empty) {
            // Process Group is empty so return true indicating that the claim is now held.
            return true;
        }

        // Process Group was not empty, so we cannot allow any more FlowFiles through. Reset claimed to false and return false,
        // indicating that the caller did not obtain the claim.
        claimed.set(false);
        return false;
    }

    @Override
    public void releaseClaim(final Port port) {
        claimed.set(false);
    }
}
