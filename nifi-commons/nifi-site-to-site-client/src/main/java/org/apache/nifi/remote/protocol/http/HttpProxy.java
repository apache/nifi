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
package org.apache.nifi.remote.protocol.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;

public class HttpProxy {
    private final String host;
    private final Integer port;
    private final String username;
    private final String password;

    public HttpProxy(final String host, final Integer port, final String username, final String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }


    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public HttpHost getHttpHost() {
        if (StringUtils.isEmpty(host)) {
            return null;
        }
        return new HttpHost(host, port == null ? 80 : port);
    }

}
