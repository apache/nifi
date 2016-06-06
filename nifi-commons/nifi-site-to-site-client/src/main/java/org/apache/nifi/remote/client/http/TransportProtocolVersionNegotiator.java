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
package org.apache.nifi.remote.client.http;

import org.apache.nifi.remote.StandardVersionNegotiator;

public class TransportProtocolVersionNegotiator extends StandardVersionNegotiator {

    public TransportProtocolVersionNegotiator(final int... supportedVersions) {
        super(supportedVersions);
    }

    /**
     * Returns a transaction protocol version for this transport protocol version.
     * This method lets transport protocol to move forward independently from transaction protocol.
     * @return a transaction protocol version
     */
    public int getTransactionProtocolVersion() {
        switch (getVersion()) {
            case 1:
                return 5;
            default:
                throw new RuntimeException("Transport protocol version " + getVersion()
                        + " was not configured with any transaction protocol version.");
        }
    }

}
