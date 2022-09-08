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
package org.apache.nifi.web.client.provider.api;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;

/**
 * Web Client Service Provider abstracts configuration of Web Client Service instances
 */
public interface WebClientServiceProvider extends ControllerService {
    /**
     * Get new HTTP URI Builder
     *
     * @return New instance of HTTP URI Builder
     */
    HttpUriBuilder getHttpUriBuilder();

    /**
     * Get Web Client Service based on current configuration
     *
     * @return Configured Web Client Service
     */
    WebClientService getWebClientService();
}
