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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class RegisteredPartition {
    private final String connectionId;
    private final Supplier<FlowFileRecord> flowFileRecordSupplier;
    private final TransactionFailureCallback failureCallback;
    private final BooleanSupplier emptySupplier;
    private final TransactionCompleteCallback successCallback;
    private final Supplier<LoadBalanceCompression> compressionSupplier;
    private final BooleanSupplier honorBackpressureSupplier;

    public RegisteredPartition(final String connectionId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier, final TransactionFailureCallback failureCallback,
                               final TransactionCompleteCallback successCallback, final Supplier<LoadBalanceCompression> compressionSupplier, final BooleanSupplier honorBackpressureSupplier) {
        this.connectionId = connectionId;
        this.emptySupplier = emptySupplier;
        this.flowFileRecordSupplier = flowFileSupplier;
        this.failureCallback = failureCallback;
        this.successCallback = successCallback;
        this.compressionSupplier = compressionSupplier;
        this.honorBackpressureSupplier = honorBackpressureSupplier;
    }

    public boolean isEmpty() {
        return emptySupplier.getAsBoolean();
    }

    public String getConnectionId() {
        return connectionId;
    }

    public Supplier<FlowFileRecord> getFlowFileRecordSupplier() {
        return flowFileRecordSupplier;
    }

    public TransactionFailureCallback getFailureCallback() {
        return failureCallback;
    }

    public TransactionCompleteCallback getSuccessCallback() {
        return successCallback;
    }

    public LoadBalanceCompression getCompression() {
        return compressionSupplier.get();
    }

    public boolean isHonorBackpressure() {
        return honorBackpressureSupplier.getAsBoolean();
    }
}
