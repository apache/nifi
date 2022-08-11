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
package org.apache.nifi.web.client.proxy;

import java.net.Proxy;
import java.util.Optional;

/**
 * Proxy Context provides information necessary to access sites through a Proxy with or without authentication
 */
public interface ProxyContext {
    /**
     * Get Proxy including Proxy Type and Proxy Server
     *
     * @return Proxy
     */
    Proxy getProxy();

    /**
     * Get Username for Proxy Authentication
     *
     * @return Username or empty when not configured
     */
    Optional<String> getUsername();

    /**
     * Get Password for Proxy Authentication
     *
     * @return Password or empty when not configured
     */
    Optional<String> getPassword();
}
