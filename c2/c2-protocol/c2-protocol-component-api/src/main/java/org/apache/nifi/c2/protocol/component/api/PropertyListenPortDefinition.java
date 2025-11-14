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
package org.apache.nifi.c2.protocol.component.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.List;

public class PropertyListenPortDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    private TransportProtocol transportProtocol;
    private List<String> applicationProtocols;

    @Schema(description = "The transport protocol used by this listen port")
    public TransportProtocol getTransportProtocol() {
        return transportProtocol;
    }

    public void setTransportProtocol(final TransportProtocol transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    @Schema(description = "The application protocols that this listen port could support (if any)")
    public List<String> getApplicationProtocols() {
        return applicationProtocols;
    }

    public void setApplicationProtocols(final List<String> applicationProtocols) {
        this.applicationProtocols = applicationProtocols;
    }

    public enum TransportProtocol {
        TCP,
        UDP
    }

}
