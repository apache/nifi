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

import { RegistryClientEntity } from '../../../../state/shared';
import { BreadcrumbEntity } from '../../../flow-designer/state/shared';

export const connectorCanvasFeatureKey = 'connectorCanvas';

export interface ConnectorCanvasState {
    connectorId: string;
    processGroupId: string | null;
    parentProcessGroupId: string | null;
    breadcrumb: BreadcrumbEntity | null;
    labels: any[];
    funnels: any[];
    inputPorts: any[];
    outputPorts: any[];
    remoteProcessGroups: any[];
    processGroups: any[];
    processors: any[];
    connections: any[];
    registryClients: RegistryClientEntity[];
    skipTransform: boolean;
    loadingStatus: 'pending' | 'loading' | 'success' | 'error';
    error: string | null;
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
    error: null
};
