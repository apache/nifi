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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg.cs;

import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.util.List;

/**
 * Utility methods for controller service commands.
 */
public class ControllerServiceUtil {

    public static ControllerServiceStateCounts getControllerServiceStates(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {
        final ControllerServicesEntity servicesEntity = flowClient.getControllerServices(pgId);
        if (servicesEntity == null || servicesEntity.getControllerServices() == null) {
            return new ControllerServiceStateCounts();
        }
        final List<ControllerServiceEntity> pgCs = servicesEntity.getControllerServices().stream().filter(c -> c.getParentGroupId().equals(pgId)).toList();
        return getControllerServiceStates(pgCs);
    }

    private static ControllerServiceStateCounts getControllerServiceStates(final List<ControllerServiceEntity> pgCs)
            throws NiFiClientException {

        final ControllerServiceStateCounts states = new ControllerServiceStateCounts();
        if (pgCs.isEmpty()) {
            return states;
        }

        for (final ControllerServiceEntity serviceEntity : pgCs) {
            final String state = serviceEntity.getComponent().getState();
            switch (state) {
            case ControllerServiceStates.STATE_ENABLED:
                states.incrementEnabled();
                break;
            case ControllerServiceStates.STATE_ENABLING:
                states.incrementEnabling();
                break;
            case ControllerServiceStates.STATE_DISABLED:
                states.incrementDisabled();
                break;
            case ControllerServiceStates.STATE_DISABLING:
                states.incrementDisabling();
                break;
            default:
                throw new NiFiClientException("Unexpected controller service state: " + state);
            }
        }

        return states;
    }

}
