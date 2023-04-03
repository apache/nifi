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
package org.apache.nifi.c2.client.api;

import java.util.Optional;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2OperationAck;

/**
 * Defines interface methods used to implement a C2 Client. The controller can be application-specific but is used for such tasks as updating the flow.
 */
public interface C2Client {

    /**
     * Responsible for sending the C2Heartbeat to the C2 Server
     *
     * @param heartbeat the heartbeat to be sent
     * @return optional response from the C2 Server if the response arrived it will be populated
     */
    Optional<C2HeartbeatResponse> publishHeartbeat(C2Heartbeat heartbeat);

    /**
     * After operation completed the acknowledgment to be sent to the C2 Server
     *
     * @param operationAck the acknowledgment details to be sent
     */
    void acknowledgeOperation(C2OperationAck operationAck);

    /**
     * Retrieve the content of the new flow from the C2 Server
     *
     * @param callbackUrl url where the content should be downloaded from
     * @return the actual downloaded content. Will be empty if no content can be downloaded
     */
    Optional<byte[]> retrieveUpdateContent(String callbackUrl);

    /**
     * Uploads a binary bundle to C2 server
     *
     * @param callbackUrl url where the content should be uploaded to
     * @param bundle      bundle content as byte array to be uploaded
     * @return optional error message if any issues occurred
     */
    Optional<String> uploadBundle(String callbackUrl, byte[] bundle);

    /**
     * Creates a callback URL according to proxy aware C2 settings
     *
     * @param absoluteUrl absolute url sent by C2 server
     * @param relativeUrl relative url sent by C2 server
     * @return an optional with content of finalised callback url
     */
    Optional<String> getCallbackUrl(String absoluteUrl, String relativeUrl);
}
