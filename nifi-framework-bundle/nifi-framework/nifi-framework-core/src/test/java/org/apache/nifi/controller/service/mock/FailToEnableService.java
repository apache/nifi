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
package org.apache.nifi.controller.service.mock;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

/**
 * A Controller Service that is valid but always throws from its {@code @OnEnabled} method, so it can never
 * successfully reach the ENABLED state. This is used to simulate a Controller Service that fails to (re)enable
 * during a flow upgrade.
 */
public class FailToEnableService extends AbstractControllerService {

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        throw new RuntimeException("Intentionally failing to enable in order to simulate a Controller Service that does not come back up during a flow upgrade");
    }
}
