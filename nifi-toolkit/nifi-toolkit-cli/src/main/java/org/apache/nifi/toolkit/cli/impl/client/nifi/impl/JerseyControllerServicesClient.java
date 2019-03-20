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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

/**
 * Jersey implementation of ControllerServicersClient.
 */
public class JerseyControllerServicesClient extends AbstractJerseyClient implements ControllerServicesClient {

    private final WebTarget controllerServicesTarget;

    public JerseyControllerServicesClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyControllerServicesClient(final WebTarget baseTarget, final Map<String, String> headers) {
        super(headers);
        this.controllerServicesTarget = baseTarget.path("/controller-services");
    }

    @Override
    public ControllerServiceEntity getControllerService(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Controller service id cannot be null");
        }

        return executeAction("Error retrieving status of controller service", () -> {
            final WebTarget target = controllerServicesTarget.path("{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity activateControllerService(final String id,
            final ControllerServiceRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Controller service id cannot be null");
        }

        if (runStatusEntity == null) {
            throw new IllegalArgumentException("Entity cannnot be null");
        }

        return executeAction("Error enabling or disabling controller service", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/run-status").resolveTemplate("id", id);
            return getRequestBuilder(target).put(
                Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                ControllerServiceEntity.class);
        });
    }
}
