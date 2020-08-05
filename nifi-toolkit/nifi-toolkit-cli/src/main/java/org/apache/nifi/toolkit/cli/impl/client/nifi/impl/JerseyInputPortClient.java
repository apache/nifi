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

import org.apache.nifi.toolkit.cli.impl.client.nifi.InputPortClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.PortEntity;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

public class JerseyInputPortClient extends CRUDJerseyClient<PortEntity> implements InputPortClient {

    public JerseyInputPortClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyInputPortClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(baseTarget.path("/process-groups/{pgId}/input-ports"),
            baseTarget.path("/input-ports/{id}"),
            requestConfig,
            PortEntity.class,
            "Input Port");
    }

    @Override
    public PortEntity createInputPort(final String parentGroupId, final PortEntity entity) throws NiFiClientException, IOException {
        return createComponent(parentGroupId, entity);
    }

    @Override
    public PortEntity getInputPort(final String id) throws NiFiClientException, IOException {
        return getComponent(id);
    }

    @Override
    public PortEntity updateInputPort(final PortEntity entity) throws NiFiClientException, IOException {
        return updateComponent(entity);
    }

    @Override
    public PortEntity deleteInputPort(final PortEntity entity) throws NiFiClientException, IOException {
        return deleteComponent(entity);
    }

    @Override
    public PortEntity startInputPort(final PortEntity entity) throws NiFiClientException, IOException {
        final PortEntity startEntity = createStateEntity(entity, "RUNNING");
        return updateInputPort(startEntity);
    }

    @Override
    public PortEntity stopInputPort(final PortEntity entity) throws NiFiClientException, IOException {
        final PortEntity startEntity = createStateEntity(entity, "STOPPED");
        return updateInputPort(startEntity);
    }

    private PortEntity createStateEntity(final PortEntity entity, final String state) {
        final PortDTO component = new PortDTO();
        component.setId(entity.getComponent().getId());
        component.setParentGroupId(entity.getComponent().getParentGroupId());
        component.setState(state);

        final PortEntity stateEntity = new PortEntity();
        stateEntity.setId(entity.getId());
        stateEntity.setRevision(entity.getRevision());
        stateEntity.setComponent(component);

        return stateEntity;
    }
}
