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
    CreateComponent,
    CreateComponentResponse,
    CreateConnection,
    CreateConnectionDialogRequest,
    CreateConnectionRequest,
    CreatePort,
    CreateProcessGroup,
    CreateProcessGroupDialogRequest,
    CreateProcessor,
    DeleteComponent,
    DeleteComponentResponse,
    EditComponent,
    EditComponentRequest,
    EditConnection,
    EnterProcessGroupRequest,
    LoadConnectionSuccess,
    LoadInputPortSuccess,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    LoadProcessorSuccess,
    LoadRemoteProcessGroupSuccess,
    NavigateToComponentRequest,
    SelectComponents,
    UpdateComponent,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdateConnection,
    UpdateConnectionSuccess,
    UpdatePositions,
    UploadProcessGroup
} from './index';

/*
    Loading Flow
 */

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
    props<{ request: SelectComponents }>()
);

export const selectComponents = createAction('[Canvas] Select Components', props<{ request: SelectComponents }>());

export const deselectAllComponents = createAction('[Canvas] Deselect All Components');

export const removeSelectedComponents = createAction(
    '[Canvas] Remove Selected Components',
    props<{ request: SelectComponents }>()
);

export const centerSelectedComponent = createAction('[Canvas] Center Selected Components');

/*
    Create Component Actions
 */

export const createComponentRequest = createAction(
    '[Canvas] Create Component Request',
    props<{ request: CreateComponent }>()
);

export const createFunnel = createAction('[Canvas] Create Funnel', props<{ request: CreateComponent }>());

export const createLabel = createAction('[Canvas] Create Label', props<{ request: CreateComponent }>());

export const openNewProcessGroupDialog = createAction(
    '[Canvas] Open New Process Group Dialog',
    props<{ request: CreateProcessGroupDialogRequest }>()
);

export const createProcessGroup = createAction(
    '[Canvas] Create Process Group',
    props<{ request: CreateProcessGroup }>()
);

export const uploadProcessGroup = createAction(
    '[Canvas] Upload Process Group',
    props<{ request: UploadProcessGroup }>()
);

export const openNewProcessorDialog = createAction(
    '[Canvas] Open New Processor Dialog',
    props<{ request: CreateComponent }>()
);

export const createProcessor = createAction('[Canvas] Create Processor', props<{ request: CreateProcessor }>());

export const getDefaultsAndOpenNewConnectionDialog = createAction(
    '[Canvas] Get Defaults And Open New Connection Dialog',
    props<{ request: CreateConnectionRequest }>()
);

export const openNewConnectionDialog = createAction(
    '[Canvas] Open New Connection Dialog',
    props<{ request: CreateConnectionDialogRequest }>()
);

export const createConnection = createAction('[Canvas] Create Connection', props<{ request: CreateConnection }>());

export const openNewPortDialog = createAction('[Canvas] Open New Port Dialog', props<{ request: CreateComponent }>());

export const createPort = createAction('[Canvas] Create Port', props<{ request: CreatePort }>());

export const createComponentSuccess = createAction(
    '[Canvas] Create Component Success',
    props<{ response: CreateComponentResponse }>()
);

export const createComponentComplete = createAction(
    '[Canvas] Create Component Complete',
    props<{ response: CreateComponentResponse }>()
);

/*
    Update Component Actions
 */

export const navigateToEditComponent = createAction(
    '[Canvas] Navigate To Edit Component',
    props<{ request: EditComponentRequest }>()
);

export const editComponent = createAction('[Canvas] Edit Component', props<{ request: EditComponent }>());

export const openEditPortDialog = createAction('[Canvas] Open Edit Port Dialog', props<{ request: EditComponent }>());

export const openEditProcessorDialog = createAction(
    '[Canvas] Open Edit Processor Dialog',
    props<{ request: EditComponent }>()
);

export const openEditConnectionDialog = createAction(
    '[Canvas] Open Edit Connection Dialog',
    props<{ request: EditConnection }>()
);

export const updateComponent = createAction('[Canvas] Update Component', props<{ request: UpdateComponent }>());

export const updateComponentSuccess = createAction(
    '[Canvas] Update Component Success',
    props<{ response: UpdateComponentResponse }>()
);

export const updateComponentFailure = createAction(
    '[Canvas] Update Component Failure',
    props<{ response: UpdateComponentFailure }>()
);

export const updateProcessor = createAction('[Canvas] Update Processor', props<{ request: UpdateComponent }>());

export const updateProcessorSuccess = createAction(
    '[Canvas] Update Processor Success',
    props<{ response: UpdateComponentResponse }>()
);

export const updateConnection = createAction('[Canvas] Update Connection', props<{ request: UpdateConnection }>());

export const updateConnectionSuccess = createAction(
    '[Canvas] Update Connection Success',
    props<{ response: UpdateConnectionSuccess }>()
);

export const updatePositions = createAction('[Canvas] Update Positions', props<{ request: UpdatePositions }>());

export const updatePositionComplete = createAction(
    '[Canvas] Update Position Complete',
    props<{ response: UpdateComponentResponse }>()
);

/*
    Delete Component Actions
 */

export const deleteComponents = createAction('[Canvas] Delete Components', props<{ request: DeleteComponent[] }>());

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
