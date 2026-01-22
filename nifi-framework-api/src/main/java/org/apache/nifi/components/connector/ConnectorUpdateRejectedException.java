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

package org.apache.nifi.components.connector;

/**
 * Exception thrown when a connector update is rejected by the {@link ConnectorRepository}.
 * This typically occurs when the repository's {@link ConnectorRepository#prepareForUpdate(String)}
 * method returns {@code false}, indicating that it's not safe to proceed with the update.
 *
 * <p>
 * This exception should result in a 409 Conflict HTTP response when propagated to the web layer,
 * indicating that the update cannot be completed at this time due to a conflict with the
 * current state of the external persistence provider.
 * </p>
 */
public class ConnectorUpdateRejectedException extends RuntimeException {

    private final String connectorId;

    /**
     * Constructs a new exception with the specified connector ID and message.
     *
     * @param connectorId the identifier of the connector whose update was rejected
     * @param message the detail message
     */
    public ConnectorUpdateRejectedException(final String connectorId, final String message) {
        super(message);
        this.connectorId = connectorId;
    }

    /**
     * Constructs a new exception with the specified connector ID, message, and cause.
     *
     * @param connectorId the identifier of the connector whose update was rejected
     * @param message the detail message
     * @param cause the cause of the rejection
     */
    public ConnectorUpdateRejectedException(final String connectorId, final String message, final Throwable cause) {
        super(message, cause);
        this.connectorId = connectorId;
    }

    /**
     * Returns the identifier of the connector whose update was rejected.
     *
     * @return the connector identifier
     */
    public String getConnectorId() {
        return connectorId;
    }
}
