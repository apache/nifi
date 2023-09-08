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
    EditComponent,
    EnterProcessGroupRequest,
    EnterProcessGroupResponse,
    UpdateComponent,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdatePositions
} from '../index';

/*
    Loading Flow
 */

export const enterProcessGroup = createAction(
    '[Canvas] Enter Process Group',
    props<{ request: EnterProcessGroupRequest }>()
);

export const enterProcessGroupSuccess = createAction(
    '[Canvas] Enter Process Group Success',
    props<{ response: EnterProcessGroupResponse }>()
);

export const enterProcessGroupComplete = createAction(
    '[Canvas] Enter Process Group Complete',
    props<{ response: EnterProcessGroupResponse }>()
);

export const flowApiError = createAction('[Canvas] Flow Api Error', props<{ error: string }>());

export const clearFlowApiError = createAction('[Canvas] Clear Flow Api Error');

/*
    Selectable Behavior
 */

export const addSelectedComponents = createAction('[Canvas] Add Selected Component', props<{ ids: string[] }>());

export const setSelectedComponents = createAction('[Canvas] Set Selected Components', props<{ ids: string[] }>());

export const removeSelectedComponents = createAction('[Canvas] Remove Selected Components', props<{ ids: string[] }>());

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
    Update Component Action
 */

export const editComponentRequest = createAction(
    '[Canvas] Edit Component Request',
    props<{ request: EditComponent }>()
);

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
    Transition
 */

export const setDragging = createAction('[Canvas] Set Dragging', props<{ dragging: boolean }>());

export const setTransitionRequired = createAction(
    '[Canvas] Set Transition Required',
    props<{ transitionRequired: boolean }>()
);

export const setRenderRequired = createAction('[Canvas] Set Render Required', props<{ renderRequired: boolean }>());
