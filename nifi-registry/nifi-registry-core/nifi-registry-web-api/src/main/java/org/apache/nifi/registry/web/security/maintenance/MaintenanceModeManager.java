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
package org.apache.nifi.registry.web.security.maintenance;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the maintenance mode state for the NiFi Registry.
 * When maintenance mode is enabled, write operations are rejected with HTTP 503.
 */
@Component
public class MaintenanceModeManager {

    private final AtomicBoolean maintenanceModeEnabled = new AtomicBoolean(false);

    public void enable() {
        maintenanceModeEnabled.set(true);
    }

    public void disable() {
        maintenanceModeEnabled.set(false);
    }

    public boolean isEnabled() {
        return maintenanceModeEnabled.get();
    }
}
