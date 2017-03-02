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
package org.apache.nifi.web.api.request;

import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/**
 * Class for parsing handling client ids. If the client id is not specified, one will be generated.
 */
public class ClientIdParameter {

    private final String clientId;

    public ClientIdParameter(String clientId) {
        if (StringUtils.isBlank(clientId)) {
            this.clientId = UUID.randomUUID().toString();
        } else {
            this.clientId = clientId;
        }
    }

    public ClientIdParameter() {
        this.clientId = UUID.randomUUID().toString();
    }

    public String getClientId() {
        return clientId;
    }
}
