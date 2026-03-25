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
package org.apache.nifi.registry.actuator;

import org.apache.nifi.registry.web.security.maintenance.MaintenanceModeManager;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Boot Actuator endpoint for managing NiFi Registry maintenance mode.
 *
 * <p>Exposed at {@code /actuator/maintenance} and secured by the existing
 * {@code ResourceAuthorizationFilter} which covers the {@code /actuator} path.
 *
 * <p>Usage:
 * <pre>
 *   GET  /actuator/maintenance           → returns {"enabled": true|false}
 *   POST /actuator/maintenance           → body {"enabled": true} enables maintenance mode
 *                                          body {"enabled": false} disables maintenance mode
 * </pre>
 */
@Component
@Endpoint(id = "maintenance")
public class MaintenanceModeEndpoint {

    private final MaintenanceModeManager maintenanceModeManager;

    public MaintenanceModeEndpoint(final MaintenanceModeManager maintenanceModeManager) {
        this.maintenanceModeManager = maintenanceModeManager;
    }

    @ReadOperation
    public Map<String, Object> getMaintenanceModeStatus() {
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("enabled", maintenanceModeManager.isEnabled());
        return status;
    }

    @WriteOperation
    public Map<String, Object> setMaintenanceModeStatus(final boolean enabled) {
        if (enabled) {
            maintenanceModeManager.enable();
        } else {
            maintenanceModeManager.disable();
        }
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("enabled", maintenanceModeManager.isEnabled());
        return status;
    }
}
