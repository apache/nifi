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
import { UpdatePositions } from '../index';

/*
    Loading Flow
 */

export const loadFlow = createAction('[Canvas] Load Flow');

export const loadFlowSuccess = createAction(
  '[Canvas] Flow Load Success',
  props<{ flow: any }>()
);

export const loadFlowFailure = createAction(
  '[Canvas] Flow Load Failure',
  props<{ error: string }>()
)

/*
    Selectable Behavior
 */

export const addSelectedComponents = createAction(
    '[Canvas] Add Selected Component',
    props<{ ids: string[] }>()
)

export const setSelectedComponents = createAction(
    '[Canvas] Set Selected Components',
    props<{ ids: string[] }>()
)

export const removeSelectedComponents = createAction(
    '[Canvas] Remove Selected Components',
    props<{ ids: string[] }>()
)

/*
    Draggable Behavior
 */

export const updatePositions = createAction(
  '[Canvas] Update Positions',
  props<{ positionUpdates: UpdatePositions }>()
)

export const updatePositionSuccess = createAction(
  '[Canvas] Update Positions Success',
  props<{ positionUpdates: UpdatePositions }>()
)

/*
    Transition
 */

export const setTransition = createAction(
    '[Canvas] Set Transition',
    props<{ transition: boolean }>()
)
