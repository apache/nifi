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

package org.apache.nifi.c2.client.http.url;

public interface C2UrlProvider {

    /**
     * Retrieves the url of the C2 server to send heartbeats to
     *
     * @return the url of the C2 server to send heartbeats to
     */
    String getHeartbeatUrl();

    /**
     * Retrieves the url of the C2 server to send acknowledgements to
     *
     * @return the url of the C2 server to send acknowledgements to
     */
    String getAcknowledgeUrl();

    /**
     * Retrieves the callback url of the C2 server according to the C2 configuration (proxy aware or not)
     *
     * @param absoluteUrl absolute url sent by the C2 server
     * @param relativeUrl relative url sent by the C2 server
     * @return the url of the C2 server to send requests to
     */
    String getCallbackUrl(String absoluteUrl, String relativeUrl);
}
