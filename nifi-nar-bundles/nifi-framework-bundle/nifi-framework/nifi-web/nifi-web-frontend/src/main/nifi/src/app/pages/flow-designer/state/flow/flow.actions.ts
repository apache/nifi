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
import {
    CreateComponentRequest,
    CreateComponentResponse,
    CreateConnection,
    CreateConnectionDialogRequest,
    CreateConnectionRequest,
    CreatePortRequest,
    CreateProcessGroupRequest,
    CreateProcessGroupDialogRequest,
    CreateProcessorRequest,
    DeleteComponentRequest,
    DeleteComponentResponse,
    EditComponentDialogRequest,
    OpenComponentDialogRequest,
    EditConnectionDialogRequest,
    EnterProcessGroupRequest,
    GroupComponentsRequest,
    GroupComponentsDialogRequest,
    OpenGroupComponentsDialogRequest,
    GroupComponentsSuccess,
    LoadConnectionSuccess,
    LoadInputPortSuccess,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    LoadProcessorSuccess,
    LoadRemoteProcessGroupSuccess,
    MoveComponentsRequest,
    NavigateToComponentRequest,
    SelectComponentsRequest,
    UpdateComponentRequest,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdateConnectionRequest,
    UpdateConnectionSuccess,
    UpdatePositionsRequest,
    UploadProcessGroupRequest,
    EditCurrentProcessGroupRequest,
    NavigateToControllerServicesRequest,
    ReplayLastProvenanceEventRequest
} from './index';
import { StatusHistoryRequest } from '../../../../state/status-history';

/*
    Loading Flow
 */

export const resetFlowState = createAction('[Canvas] Reset Flow State');

export const reloadFlow = createAction('[Canvas] Reload Flow');

export const leaveProcessGroup = createAction('[Canvas] Leave Process Group');

export const enterProcessGroup = createAction(
    '[Canvas] Enter Process Group',
    props<{ request: EnterProcessGroupRequest }>()
);

export const loadProcessGroup = createAction(
    '[Canvas] Load Process Group',
    props<{ request: LoadProcessGroupRequest }>()
);

export const loadProcessGroupSuccess = createAction(
    '[Canvas] Load Process Group Success',
    props<{ response: LoadProcessGroupResponse }>()
);

export const loadProcessGroupComplete = createAction(
    '[Canvas] Load Process Group Complete',
    props<{ response: LoadProcessGroupResponse }>()
);

export const flowApiError = createAction('[Canvas] Flow Api Error', props<{ error: string }>());

export const clearFlowApiError = createAction('[Canvas] Clear Flow Api Error');

export const startProcessGroupPolling = createAction('[Canvas] Start Process Group Polling');

export const stopProcessGroupPolling = createAction('[Canvas] Stop Process Group Polling');

export const loadConnectionsForComponent = createAction(
    '[Canvas] Load Connections For Component',
    props<{ id: string }>()
);

export const loadConnection = createAction('[Canvas] Load Connection', props<{ id: string }>());

export const loadConnectionSuccess = createAction(
    '[Canvas] Load Connection Success',
    props<{ response: LoadConnectionSuccess }>()
);

export const loadComponentsForConnection = createAction(
    '[Canvas] Load Components For Connection',
    props<{ connection: any }>()
);

export const loadProcessor = createAction('[Canvas] Load Processor', props<{ id: string }>());

export const loadProcessorSuccess = createAction(
    '[Canvas] Load Processor Success',
    props<{ response: LoadProcessorSuccess }>()
);

export const loadInputPort = createAction('[Canvas] Load Input Port', props<{ id: string }>());

export const loadInputPortSuccess = createAction(
    '[Canvas] Load Input Port Success',
    props<{ response: LoadInputPortSuccess }>()
);

export const loadRemoteProcessGroup = createAction('[Canvas] Load Remote Process Group', props<{ id: string }>());

export const loadRemoteProcessGroupSuccess = createAction(
    '[Canvas] Load Remote Process Group Success',
    props<{ response: LoadRemoteProcessGroupSuccess }>()
);

/*
    Selectable Behavior
 */

export const addSelectedComponents = createAction(
    '[Canvas] Add Selected Component',
    props<{ request: SelectComponentsRequest }>()
);

export const selectComponents = createAction(
    '[Canvas] Select Components',
    props<{ request: SelectComponentsRequest }>()
);

export const deselectAllComponents = createAction('[Canvas] Deselect All Components');

export const removeSelectedComponents = createAction(
    '[Canvas] Remove Selected Components',
    props<{ request: SelectComponentsRequest }>()
);

export const centerSelectedComponent = createAction('[Canvas] Center Selected Component');

/*
    Create Component Actions
 */

export const createComponentRequest = createAction(
    '[Canvas] Create Component Request',
    props<{ request: CreateComponentRequest }>()
);

export const createFunnel = createAction('[Canvas] Create Funnel', props<{ request: CreateComponentRequest }>());

export const createLabel = createAction('[Canvas] Create Label', props<{ request: CreateComponentRequest }>());

export const openNewProcessGroupDialog = createAction(
    '[Canvas] Open New Process Group Dialog',
    props<{ request: CreateProcessGroupDialogRequest }>()
);

export const createProcessGroup = createAction(
    '[Canvas] Create Process Group',
    props<{ request: CreateProcessGroupRequest }>()
);

export const uploadProcessGroup = createAction(
    '[Canvas] Upload Process Group',
    props<{ request: UploadProcessGroupRequest }>()
);

export const getParameterContextsAndOpenGroupComponentsDialog = createAction(
    '[Canvas] Get Parameter Contexts And Open Group Components Dialog',
    props<{ request: OpenGroupComponentsDialogRequest }>()
);

export const openGroupComponentsDialog = createAction(
    '[Canvas] Open Group Components Dialog',
    props<{ request: GroupComponentsDialogRequest }>()
);

export const groupComponents = createAction('[Canvas] Group Components', props<{ request: GroupComponentsRequest }>());

export const groupComponentsSuccess = createAction(
    '[Canvas] Group Components Success',
    props<{ response: GroupComponentsSuccess }>()
);

export const openNewProcessorDialog = createAction(
    '[Canvas] Open New Processor Dialog',
    props<{ request: CreateComponentRequest }>()
);

export const createProcessor = createAction('[Canvas] Create Processor', props<{ request: CreateProcessorRequest }>());

export const getDefaultsAndOpenNewConnectionDialog = createAction(
    '[Canvas] Get Defaults And Open New Connection Dialog',
    props<{ request: CreateConnectionRequest }>()
);

export const openNewConnectionDialog = createAction(
    '[Canvas] Open New Connection Dialog',
    props<{ request: CreateConnectionDialogRequest }>()
);

export const createConnection = createAction('[Canvas] Create Connection', props<{ request: CreateConnection }>());

export const openNewPortDialog = createAction(
    '[Canvas] Open New Port Dialog',
    props<{ request: CreateComponentRequest }>()
);

export const createPort = createAction('[Canvas] Create Port', props<{ request: CreatePortRequest }>());

export const createComponentSuccess = createAction(
    '[Canvas] Create Component Success',
    props<{ response: CreateComponentResponse }>()
);

export const createComponentComplete = createAction(
    '[Canvas] Create Component Complete',
    props<{ response: CreateComponentResponse }>()
);

export const navigateToViewStatusHistoryForComponent = createAction(
    '[Canvas] Navigate To Status History For Component',
    props<{ request: OpenComponentDialogRequest }>()
);

export const viewStatusHistoryForComponent = createAction(
    '[Canvas] View Status History for Component',
    props<{ request: StatusHistoryRequest }>()
);
/*
    Update Component Actions
 */

export const navigateToEditComponent = createAction(
    '[Canvas] Navigate To Edit Component',
    props<{ request: OpenComponentDialogRequest }>()
);

export const editComponent = createAction('[Canvas] Edit Component', props<{ request: EditComponentDialogRequest }>());

export const navigateToEditCurrentProcessGroup = createAction('[Canvas] Navigate To Edit Current Process Group');

export const navigateToControllerServicesForProcessGroup = createAction(
    '[Canvas] Navigate To Controller Services For Process Group',
    props<{ request: NavigateToControllerServicesRequest }>()
);

export const editCurrentProcessGroup = createAction(
    '[Canvas] Edit Current Process Group',
    props<{
        request: EditCurrentProcessGroupRequest;
    }>()
);

export const openEditPortDialog = createAction(
    '[Canvas] Open Edit Port Dialog',
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditProcessorDialog = createAction(
    '[Canvas] Open Edit Processor Dialog',
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditConnectionDialog = createAction(
    '[Canvas] Open Edit Connection Dialog',
    props<{ request: EditConnectionDialogRequest }>()
);

export const openEditProcessGroupDialog = createAction(
    '[Canvas] Open Edit Process Group Dialog',
    props<{ request: EditComponentDialogRequest }>()
);

export const updateComponent = createAction('[Canvas] Update Component', props<{ request: UpdateComponentRequest }>());

export const updateComponentSuccess = createAction(
    '[Canvas] Update Component Success',
    props<{ response: UpdateComponentResponse }>()
);

export const updateComponentFailure = createAction(
    '[Canvas] Update Component Failure',
    props<{ response: UpdateComponentFailure }>()
);

export const updateProcessor = createAction('[Canvas] Update Processor', props<{ request: UpdateComponentRequest }>());

export const updateProcessorSuccess = createAction(
    '[Canvas] Update Processor Success',
    props<{ response: UpdateComponentResponse }>()
);

export const updateConnection = createAction(
    '[Canvas] Update Connection',
    props<{ request: UpdateConnectionRequest }>()
);

export const updateConnectionSuccess = createAction(
    '[Canvas] Update Connection Success',
    props<{ response: UpdateConnectionSuccess }>()
);

export const updatePositions = createAction('[Canvas] Update Positions', props<{ request: UpdatePositionsRequest }>());

export const updatePositionComplete = createAction(
    '[Canvas] Update Position Complete',
    props<{ response: UpdateComponentResponse }>()
);

export const moveComponents = createAction('[Canvas] Move Components', props<{ request: MoveComponentsRequest }>());

/*
    Delete Component Actions
 */

export const deleteComponents = createAction(
    '[Canvas] Delete Components',
    props<{ request: DeleteComponentRequest[] }>()
);

export const deleteComponentsSuccess = createAction(
    '[Canvas] Delete Components Success',
    props<{ response: DeleteComponentResponse[] }>()
);

/*
    Transition
 */

export const setDragging = createAction('[Canvas] Set Dragging', props<{ dragging: boolean }>());

export const setTransitionRequired = createAction(
    '[Canvas] Set Transition Required',
    props<{ transitionRequired: boolean }>()
);

export const setSkipTransform = createAction('[Canvas] Set Skip Transform', props<{ skipTransform: boolean }>());

export const navigateToComponent = createAction(
    '[Canvas] Navigate To Component',
    props<{ request: NavigateToComponentRequest }>()
);

export const navigateWithoutTransform = createAction('[Canvas] Navigate Without Transform', props<{ url: string[] }>());

/*
    Palette actions
 */

export const setNavigationCollapsed = createAction(
    '[Canvas] Set Navigation Collapsed',
    props<{ navigationCollapsed: boolean }>()
);

export const setOperationCollapsed = createAction(
    '[Canvas] Set Operation Collapsed',
    props<{ operationCollapsed: boolean }>()
);

/*
    General
 */

export const showOkDialog = createAction('[Canvas] Show Ok Dialog', props<{ title: string; message: string }>());

export const renderConnectionsForComponent = createAction(
    '[Canvas] Render Connections For Component',
    props<{ id: string; updatePath: boolean; updateLabel: boolean }>()
);

export const navigateToProvenanceForComponent = createAction(
    '[Canvas] Navigate To Provenance For Component',
    props<{ id: string }>()
);

export const replayLastProvenanceEvent = createAction(
    '[Canvas] Replay Last Provenance Event',
    props<{ request: ReplayLastProvenanceEventRequest }>()
);
