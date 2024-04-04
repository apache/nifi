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

const QUEUE_PREFIX = '[Queue]';

export const queueApiError = createAction(`${QUEUE_PREFIX} Queue Error`, props<{ error: string }>());

export const resetQueueState = createAction(`${QUEUE_PREFIX} Reset Queue State`);

export const promptEmptyQueueRequest = createAction(
    `${QUEUE_PREFIX} Prompt Empty Queue Request`,
    props<{ request: SubmitEmptyQueueRequest }>()
);

export const submitEmptyQueueRequest = createAction(
    `${QUEUE_PREFIX} Submit Empty Queue Request`,
    props<{ request: SubmitEmptyQueueRequest }>()
);

export const promptEmptyQueuesRequest = createAction(
    `${QUEUE_PREFIX} Prompt Empty Queues Request`,
    props<{ request: SubmitEmptyQueuesRequest }>()
);

export const submitEmptyQueuesRequest = createAction(
    `${QUEUE_PREFIX} Submit Empty Queues Request`,
    props<{ request: SubmitEmptyQueuesRequest }>()
);

export const submitEmptyQueueRequestSuccess = createAction(
    `${QUEUE_PREFIX} Submit Empty Queue Request Success`,
    props<{ response: PollEmptyQueueSuccess }>()
);

export const startPollingEmptyQueueRequest = createAction(`${QUEUE_PREFIX} Start Polling Empty Queue Request`);

export const pollEmptyQueueRequest = createAction(`${QUEUE_PREFIX} Poll Empty Queue Request`);

export const pollEmptyQueueRequestSuccess = createAction(
    `${QUEUE_PREFIX} Poll Empty Queue Request Success`,
    props<{ response: PollEmptyQueueSuccess }>()
);

export const stopPollingEmptyQueueRequest = createAction(`${QUEUE_PREFIX} Stop Polling Empty Queue Request`);

export const deleteEmptyQueueRequest = createAction(`${QUEUE_PREFIX} Delete Empty Queue Request`);

export const showEmptyQueueResults = createAction(
    `${QUEUE_PREFIX} Show Empty Queue Results`,
    props<{ request: ShowEmptyQueueResults }>()
);
