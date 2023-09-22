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
    CreatePort,
    DeleteComponent,
    DeleteComponentResponse,
    EditComponent,
    EditComponentRequest,
    EnterProcessGroupRequest,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    SelectComponents,
    UpdateComponent,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdatePositions
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

/*
    Create Component Actions
 */

export const createComponentRequest = createAction(
    '[Canvas] Create Component Request',
    props<{ request: CreateComponent }>()
);

export const createFunnel = createAction('[Canvas] Create Funnel', props<{ request: CreateComponent }>());

export const createLabel = createAction('[Canvas] Create Label', props<{ request: CreateComponent }>());

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

export const updateComponent = createAction('[Canvas] Update Component', props<{ request: UpdateComponent }>());

export const updateComponentSuccess = createAction(
    '[Canvas] Update Component Success',
    props<{ response: UpdateComponentResponse }>()
);

export const updateComponentFailure = createAction(
    '[Canvas] Update Component Failure',
    props<{ response: UpdateComponentFailure }>()
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

export const setRenderRequired = createAction('[Canvas] Set Render Required', props<{ renderRequired: boolean }>());
