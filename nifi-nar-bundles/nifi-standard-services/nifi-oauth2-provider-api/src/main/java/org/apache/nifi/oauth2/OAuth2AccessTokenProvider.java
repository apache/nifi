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
package org.apache.nifi.oauth2;

import org.apache.nifi.controller.ControllerService;

/**
 * Controller service that provides OAuth2 access details
 */
public interface OAuth2AccessTokenProvider extends ControllerService {

    /**
     * @return A valid access token (refreshed automatically if needed) and additional metadata (provided by the OAuth2 access server)
     */
    AccessToken getAccessDetails();
}
