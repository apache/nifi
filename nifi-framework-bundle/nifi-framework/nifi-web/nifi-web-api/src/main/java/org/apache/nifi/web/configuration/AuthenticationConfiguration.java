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
package org.apache.nifi.web.configuration;

import java.net.URI;

/**
 * Authentication Configuration based on configured application properties
 *
 * @param externalLoginRequired Whether login through an external Identity Provider is required
 * @param loginSupported Whether login operations are supported
 * @param loginUri Optional URI for login operations
 * @param logoutUri Optional URI for logout operations
 */
public record AuthenticationConfiguration(
        boolean externalLoginRequired,
        boolean loginSupported,
        URI loginUri,
        URI logoutUri
) {
}
