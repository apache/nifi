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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.BulletinMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.nifi.cluster.manager.BulletinMerger.BULLETIN_COMPARATOR;
import static org.apache.nifi.reporting.BulletinRepository.MAX_BULLETINS_FOR_CONTROLLER;
import static org.apache.nifi.reporting.BulletinRepository.MAX_BULLETINS_PER_COMPONENT;

public class ControllerBulletinsEndpointMerger extends AbstractSingleEntityEndpoint<ControllerBulletinsEntity> implements EndpointResponseMerger {
    public static final Pattern CONTROLLER_BULLETINS_URI_PATTERN = Pattern.compile("/nifi-api/flow/controller/bulletins");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CONTROLLER_BULLETINS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ControllerBulletinsEntity> getEntityClass() {
        return ControllerBulletinsEntity.class;
    }

    @Override
    protected void mergeResponses(ControllerBulletinsEntity clientEntity, Map<NodeIdentifier, ControllerBulletinsEntity> entityMap,
                                  Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        final Map<NodeIdentifier, List<BulletinEntity>> bulletinDtos = new HashMap<>();
        final Map<NodeIdentifier, List<BulletinEntity>> controllerServiceBulletinDtos = new HashMap<>();
        final Map<NodeIdentifier, List<BulletinEntity>> reportingTaskBulletinDtos = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ControllerBulletinsEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final ControllerBulletinsEntity entity = entry.getValue();
            final String nodeAddress = nodeIdentifier.getApiAddress() + ":" + nodeIdentifier.getApiPort();

            // consider the bulletins if present and authorized
            if (entity.getBulletins() != null) {
                entity.getBulletins().forEach(bulletin -> {
                    if (bulletin.getNodeAddress() == null) {
                        bulletin.setNodeAddress(nodeAddress);
                    }

                    bulletinDtos.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
                });
            }
            if (entity.getControllerServiceBulletins() != null) {
                entity.getControllerServiceBulletins().forEach(bulletin -> {
                    if (bulletin.getNodeAddress() == null) {
                        bulletin.setNodeAddress(nodeAddress);
                    }

                    controllerServiceBulletinDtos.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
                });
            }
            if (entity.getReportingTaskBulletins() != null) {
                entity.getReportingTaskBulletins().forEach(bulletin -> {
                    if (bulletin.getNodeAddress() == null) {
                        bulletin.setNodeAddress(nodeAddress);
                    }

                    reportingTaskBulletinDtos.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
                });
            }
        }

        clientEntity.setBulletins(BulletinMerger.mergeBulletins(bulletinDtos, entityMap.size()));
        clientEntity.setControllerServiceBulletins(BulletinMerger.mergeBulletins(controllerServiceBulletinDtos, entityMap.size()));
        clientEntity.setReportingTaskBulletins(BulletinMerger.mergeBulletins(reportingTaskBulletinDtos, entityMap.size()));

        // sort the bulletins
        Collections.sort(clientEntity.getBulletins(), BULLETIN_COMPARATOR);
        Collections.sort(clientEntity.getControllerServiceBulletins(), BULLETIN_COMPARATOR);
        Collections.sort(clientEntity.getReportingTaskBulletins(), BULLETIN_COMPARATOR);

        // prune the response to only include the max number of bulletins
        if (clientEntity.getBulletins().size() > MAX_BULLETINS_FOR_CONTROLLER) {
            clientEntity.setBulletins(clientEntity.getBulletins().subList(0, MAX_BULLETINS_FOR_CONTROLLER));
        }
        if (clientEntity.getControllerServiceBulletins().size() > MAX_BULLETINS_PER_COMPONENT) {
            clientEntity.setControllerServiceBulletins(clientEntity.getControllerServiceBulletins().subList(0, MAX_BULLETINS_PER_COMPONENT));
        }
        if (clientEntity.getReportingTaskBulletins().size() > MAX_BULLETINS_PER_COMPONENT) {
            clientEntity.setReportingTaskBulletins(clientEntity.getReportingTaskBulletins().subList(0, MAX_BULLETINS_PER_COMPONENT));
        }
    }

}
