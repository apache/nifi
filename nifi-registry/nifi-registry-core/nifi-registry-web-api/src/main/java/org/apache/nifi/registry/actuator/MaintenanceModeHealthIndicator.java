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
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Spring Boot Actuator health indicator that surfaces maintenance mode state.
 *
 * <p>Adds a {@code maintenanceMode} detail to {@code /actuator/health} so that
 * load balancers and monitoring tools can detect when the instance is in maintenance mode.
 *
 * <p>The overall health status remains {@code UP} regardless of maintenance mode because
 * the instance is still running and serving read traffic. Operators may configure their
 * load balancer to check the {@code maintenanceMode} detail to stop routing write traffic.
 */
@Component
public class MaintenanceModeHealthIndicator implements HealthIndicator {

    private final MaintenanceModeManager maintenanceModeManager;

    public MaintenanceModeHealthIndicator(final MaintenanceModeManager maintenanceModeManager) {
        this.maintenanceModeManager = maintenanceModeManager;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("maintenanceMode", maintenanceModeManager.isEnabled())
                .build();
    }
}
