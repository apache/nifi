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
package org.apache.nifi.kafka.service.api.producer;

public class ProducerConfiguration {
    private final boolean transactionsEnabled;
    private final String transactionIdPrefix;
    private final String deliveryGuarantee;
    private final String compressionCodec;
    private final String partitionClass;


    public ProducerConfiguration(final boolean transactionsEnabled,
                                 final String transactionIdPrefix,
                                 final String deliveryGuarantee,
                                 final String compressionCodec,
                                 final String partitionClass) {
        this.transactionsEnabled = transactionsEnabled;
        this.transactionIdPrefix = transactionIdPrefix;
        this.deliveryGuarantee = deliveryGuarantee;
        this.compressionCodec = compressionCodec;
        this.partitionClass = partitionClass;
    }

    public boolean getTransactionsEnabled() {
        return transactionsEnabled;
    }

    public String getTransactionIdPrefix() {
        return transactionIdPrefix;
    }

    public String getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public String getCompressionCodec() {
        return compressionCodec;
    }

    public String getPartitionClass() {
        return partitionClass;
    }
}
