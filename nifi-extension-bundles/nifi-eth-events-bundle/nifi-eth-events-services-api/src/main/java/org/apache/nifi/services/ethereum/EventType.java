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
package org.apache.nifi.services.ethereum;

/**
 * Beacon chain event types supported by the Controller Service API.
 * This enum intentionally mirrors the external listener library values
 * without introducing a direct dependency in the API module.
 */
public enum EventType {
    HEAD,
    BLOCK,
    BLOCK_GOSSIP,
    ATTESTATION,
    SINGLE_ATTESTATION,
    VOLUNTARY_EXIT,
    BLS_TO_EXECUTION_CHANGE,
    PROPOSER_SLASHING,
    ATTESTER_SLASHING,
    FINALIZED_CHECKPOINT,
    CHAIN_REORG,
    CONTRIBUTION_AND_PROOF,
    LIGHT_CLIENT_FINALITY_UPDATE,
    LIGHT_CLIENT_OPTIMISTIC_UPDATE,
    PAYLOAD_ATTRIBUTES,
    BLOB_SIDECAR
}
