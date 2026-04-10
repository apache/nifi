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

import { createAction, props } from '@ngrx/store';
import { BreadcrumbEntity } from '../../../flow-designer/state/shared';
import { ErrorContext } from '../../../../state/error';

export const loadConnectorFlow = createAction(
    '[Connector Canvas] Load Connector Flow',
    props<{ connectorId: string; processGroupId: string }>()
);

export const loadConnectorFlowSuccess = createAction(
    '[Connector Canvas] Load Connector Flow Success',
    props<{
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
    }>()
);

export const loadConnectorFlowFailure = createAction(
    '[Connector Canvas] Load Connector Flow Failure',
    props<{ errorContext: ErrorContext }>()
);

export const loadConnectorFlowComplete = createAction('[Connector Canvas] Load Connector Flow Complete');

export const enterProcessGroup = createAction(
    '[Connector Canvas] Enter Process Group',
    props<{ request: { id: string } }>()
);

export const leaveProcessGroup = createAction('[Connector Canvas] Leave Process Group');

export const setSkipTransform = createAction(
    '[Connector Canvas] Set Skip Transform',
    props<{ skipTransform: boolean }>()
);

export const resetConnectorCanvasState = createAction('[Connector Canvas] Reset State');
