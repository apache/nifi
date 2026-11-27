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
import { DrainBlockingComponent } from '@nifi/shared';
import { DrainRequestSuccess, SubmitDrainRequest, ViewDrainRequest } from './index';

const PROCESS_GROUP_DRAIN_PREFIX = '[Process Group Drain]';

export const drainApiError = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} API Error`, props<{ error: string }>());

export const resetDrainState = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Reset State`);

export const promptDrain = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Prompt Drain`,
    props<{ request: SubmitDrainRequest }>()
);

export const beginDrain = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Begin Drain`,
    props<{ request: SubmitDrainRequest }>()
);

export const beginDrainSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Begin Drain Success`,
    props<{ response: DrainRequestSuccess }>()
);

export const viewDrain = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} View Drain`,
    props<{ request: ViewDrainRequest }>()
);

export const startPollingDrain = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Start Polling Drain`);

export const pollDrain = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Poll Drain`);

export const pollDrainSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Poll Drain Success`,
    props<{ response: DrainRequestSuccess }>()
);

export const drainEnded = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Drain Ended`);

export const cancelDrain = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Cancel Drain`);

export const cancelDrainSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Cancel Drain Success`,
    props<{ response: DrainRequestSuccess }>()
);

export const goToBlockingComponent = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Go To Blocking Component`,
    props<{ component: DrainBlockingComponent }>()
);
