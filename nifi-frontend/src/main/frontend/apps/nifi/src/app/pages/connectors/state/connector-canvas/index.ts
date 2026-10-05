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

import {
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity
} from '@nifi/shared';
import { BreadcrumbEntity, ParameterContextEntity, RegistryClientEntity } from '../../../../state/shared';

export const connectorCanvasFeatureKey = 'connectorCanvas';

export type ConnectorCanvasComponentEntity =
    | ProcessorEntity
    | PortEntity
    | ProcessGroupEntity
    | RemoteProcessGroupEntity
    | (FunnelEntity & {
          dimensions?: never;
          zIndex?: never;
      })
    | ConnectionEntity
    | LabelEntity;

export interface ConnectorCanvasState {
    connectorId: string;
    processGroupId: string | null;
    parentProcessGroupId: string | null;
    breadcrumb: BreadcrumbEntity | null;
    labels: LabelEntity[];
    funnels: FunnelEntity[];
    inputPorts: PortEntity[];
    outputPorts: PortEntity[];
    remoteProcessGroups: RemoteProcessGroupEntity[];
    processGroups: ProcessGroupEntity[];
    processors: ProcessorEntity[];
    connections: ConnectionEntity[];
    registryClients: RegistryClientEntity[];
    skipTransform: boolean;
    loadingStatus: 'pending' | 'loading' | 'success' | 'error';
    error: string | null;
    parameterContext: ParameterContextEntity | null;
}

export const initialConnectorCanvasState: ConnectorCanvasState = {
    connectorId: '',
    processGroupId: null,
    parentProcessGroupId: null,
    breadcrumb: null,
    labels: [],
    funnels: [],
    inputPorts: [],
    outputPorts: [],
    remoteProcessGroups: [],
    processGroups: [],
    processors: [],
    connections: [],
    registryClients: [],
    skipTransform: false,
    loadingStatus: 'pending',
    error: null,
    parameterContext: null
};
