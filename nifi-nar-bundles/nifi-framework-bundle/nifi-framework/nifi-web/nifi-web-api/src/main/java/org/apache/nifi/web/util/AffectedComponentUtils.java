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

package org.apache.nifi.web.util;

import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import java.util.Optional;

public class AffectedComponentUtils {

    public static AffectedComponentEntity updateEntity(final AffectedComponentEntity componentEntity, final NiFiServiceFacade serviceFacade, final DtoFactory dtoFactory) {

        switch (componentEntity.getComponent().getReferenceType()) {
            case AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR:
                final ProcessorEntity procEntity = serviceFacade.getProcessor(componentEntity.getId());
                return dtoFactory.createAffectedComponentEntity(procEntity);
            case AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT: {
                final PortEntity portEntity = serviceFacade.getInputPort(componentEntity.getId());
                return dtoFactory.createAffectedComponentEntity(portEntity, AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT);
            }
            case AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT: {
                final PortEntity portEntity = serviceFacade.getOutputPort(componentEntity.getId());
                return dtoFactory.createAffectedComponentEntity(portEntity, AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT);
            }
            case AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE:
                final ControllerServiceEntity serviceEntity = serviceFacade.getControllerService(componentEntity.getId());
                return dtoFactory.createAffectedComponentEntity(serviceEntity);
            case AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT: {
                final RemoteProcessGroupEntity remoteGroupEntity = serviceFacade.getRemoteProcessGroup(componentEntity.getComponent().getProcessGroupId());
                final RemoteProcessGroupContentsDTO remoteGroupContents = remoteGroupEntity.getComponent().getContents();
                final Optional<RemoteProcessGroupPortDTO> portDtoOption = remoteGroupContents.getInputPorts().stream()
                    .filter(port -> port.getId().equals(componentEntity.getId()))
                    .findFirst();

                if (portDtoOption.isPresent()) {
                    final RemoteProcessGroupPortDTO portDto = portDtoOption.get();
                    return dtoFactory.createAffectedComponentEntity(portDto, AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT, remoteGroupEntity);
                }
                break;
            }
            case AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT: {
                final RemoteProcessGroupEntity remoteGroupEntity = serviceFacade.getRemoteProcessGroup(componentEntity.getComponent().getProcessGroupId());
                final RemoteProcessGroupContentsDTO remoteGroupContents = remoteGroupEntity.getComponent().getContents();
                final Optional<RemoteProcessGroupPortDTO> portDtoOption = remoteGroupContents.getOutputPorts().stream()
                    .filter(port -> port.getId().equals(componentEntity.getId()))
                    .findFirst();

                if (portDtoOption.isPresent()) {
                    final RemoteProcessGroupPortDTO portDto = portDtoOption.get();
                    return dtoFactory.createAffectedComponentEntity(portDto, AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT, remoteGroupEntity);
                }
                break;
            }
        }

        return null;
    }

}
