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
    PollEmptyQueueSuccess,
    ShowEmptyQueueResults,
    SubmitEmptyQueueRequest,
    SubmitEmptyQueuesRequest
} from './index';
import { CanvasActionSource } from '../shared';

const EMPTY_QUEUE_PREFIX = '[Empty Queue]';

export const emptyQueueApiError = createAction(`${EMPTY_QUEUE_PREFIX} API Error`, props<{ error: string }>());

export const resetEmptyQueueState = createAction(`${EMPTY_QUEUE_PREFIX} Reset State`);

export const promptEmptyQueueRequest = createAction(
    `${EMPTY_QUEUE_PREFIX} Prompt Empty Queue Request`,
    props<{ request: SubmitEmptyQueueRequest }>()
);

export const submitEmptyQueueRequest = createAction(
    `${EMPTY_QUEUE_PREFIX} Submit Empty Queue Request`,
    props<{ request: SubmitEmptyQueueRequest }>()
);

export const promptEmptyQueuesRequest = createAction(
    `${EMPTY_QUEUE_PREFIX} Prompt Empty Queues Request`,
    props<{ request: SubmitEmptyQueuesRequest }>()
);

export const submitEmptyQueuesRequest = createAction(
    `${EMPTY_QUEUE_PREFIX} Submit Empty Queues Request`,
    props<{ request: SubmitEmptyQueuesRequest }>()
);

export const submitEmptyQueueRequestSuccess = createAction(
    `${EMPTY_QUEUE_PREFIX} Submit Empty Queue Request Success`,
    props<{ response: PollEmptyQueueSuccess }>()
);

export const submitEmptyQueuesRequestSuccess = createAction(
    `${EMPTY_QUEUE_PREFIX} Submit Empty Queues Request Success`,
    props<{ response: PollEmptyQueueSuccess }>()
);

export const startPollingEmptyQueueRequest = createAction(`${EMPTY_QUEUE_PREFIX} Start Polling Empty Queue Request`);

export const pollEmptyQueueRequest = createAction(`${EMPTY_QUEUE_PREFIX} Poll Empty Queue Request`);

export const pollEmptyQueueRequestSuccess = createAction(
    `${EMPTY_QUEUE_PREFIX} Poll Empty Queue Request Success`,
    props<{ response: PollEmptyQueueSuccess }>()
);

export const stopPollingEmptyQueueRequest = createAction(`${EMPTY_QUEUE_PREFIX} Stop Polling Empty Queue Request`);

export const deleteEmptyQueueRequest = createAction(`${EMPTY_QUEUE_PREFIX} Delete Empty Queue Request`);

export const showEmptyQueueResults = createAction(
    `${EMPTY_QUEUE_PREFIX} Show Empty Queue Results`,
    props<{ request: ShowEmptyQueueResults }>()
);

/**
 * Event dispatched when a queue (or all queues in a process group) has been emptied.
 * Page-specific effects (flow-designer, connector-canvas) subscribe to this and filter
 * on `source` to refresh their respective views.
 */
export const queueEmptied = createAction(
    `${EMPTY_QUEUE_PREFIX} Queue Emptied`,
    props<{ connectionId: string | null; processGroupId: string | null; source: CanvasActionSource }>()
);
