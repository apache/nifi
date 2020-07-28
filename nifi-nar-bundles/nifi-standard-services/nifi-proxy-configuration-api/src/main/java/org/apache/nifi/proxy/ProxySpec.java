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
package org.apache.nifi.proxy;

import java.net.Proxy;

public enum ProxySpec {

    HTTP(Proxy.Type.HTTP, "HTTP"),
    HTTP_AUTH(Proxy.Type.HTTP, "HTTP + AuthN"),
    SOCKS(Proxy.Type.SOCKS, "SOCKS"),
    SOCKS_AUTH(Proxy.Type.SOCKS, "SOCKS + AuthN");

    private Proxy.Type proxyType;
    private String displayName;

    ProxySpec(Proxy.Type type, String displayName) {
        this.proxyType = type;
        this.displayName = displayName;
    }

    public Proxy.Type getProxyType() {
        return proxyType;
    }

    public String getDisplayName() {
        return displayName;
    }
}
