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

import { createSelector } from '@ngrx/store';
import { DrainProcessGroupState, processGroupDrainFeatureKey } from './index';
import { CanvasState, selectCanvasState } from '../index';
import { DrainDialogState } from '../../../../ui/common/drain-process-group-dialog/drain-process-group-dialog.types';

export const selectDrainProcessGroupState = createSelector(
    selectCanvasState,
    (state: CanvasState) => state[processGroupDrainFeatureKey]
);

/**
 * The view model the drain dialog renders. Only the status and drain request are exposed, so the
 * dialog does not depend on the page's full slice.
 */
export const selectDrainDialogState = createSelector(
    selectDrainProcessGroupState,
    (state: DrainProcessGroupState): DrainDialogState => ({
        status: state.status,
        drainRequest: state.drainRequest
    })
);

export const selectDrainProcessGroupId = createSelector(
    selectDrainProcessGroupState,
    (state: DrainProcessGroupState) => state.processGroupId
);

export const selectDrainRequestId = createSelector(
    selectDrainProcessGroupState,
    (state: DrainProcessGroupState) => state.drainRequestId
);

export const selectDrainRequest = createSelector(
    selectDrainProcessGroupState,
    (state: DrainProcessGroupState) => state.drainRequest
);

export const selectDrainStatus = createSelector(
    selectDrainProcessGroupState,
    (state: DrainProcessGroupState) => state.status
);
