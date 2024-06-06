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
    DownloadFlowFileContentRequest,
    FlowFileDialogRequest,
    LoadConnectionLabelRequest,
    LoadConnectionLabelResponse,
    PollQueueListingSuccess,
    SubmitQueueListingRequest,
    ViewFlowFileContentRequest,
    ViewFlowFileRequest
} from './index';

const QUEUE_PREFIX = '[Queue Listing]';

export const loadConnectionLabel = createAction(
    `${QUEUE_PREFIX} Load Connection Label`,
    props<{ request: LoadConnectionLabelRequest }>()
);

export const loadConnectionLabelSuccess = createAction(
    `${QUEUE_PREFIX} Load Connection Label Success`,
    props<{ response: LoadConnectionLabelResponse }>()
);

export const queueListingApiError = createAction(`${QUEUE_PREFIX} Queue Error`, props<{ error: string }>());

export const resetQueueListingState = createAction(`${QUEUE_PREFIX} Reset Queue Listing State`);

export const submitQueueListingRequest = createAction(
    `${QUEUE_PREFIX} Submit Queue Listing Request`,
    props<{ request: SubmitQueueListingRequest }>()
);

export const resubmitQueueListingRequest = createAction(`${QUEUE_PREFIX} Resubmit Queue Listing Request`);

export const submitQueueListingRequestSuccess = createAction(
    `${QUEUE_PREFIX} Submit Queue Listing Request Success`,
    props<{ response: PollQueueListingSuccess }>()
);

export const startPollingQueueListingRequest = createAction(`${QUEUE_PREFIX} Start Polling Queue Listing Request`);

export const pollQueueListingRequest = createAction(`${QUEUE_PREFIX} Poll Queue Listing Request`);

export const pollQueueListingRequestSuccess = createAction(
    `${QUEUE_PREFIX} Poll Queue Listing Request Success`,
    props<{ response: PollQueueListingSuccess }>()
);

export const stopPollingQueueListingRequest = createAction(`${QUEUE_PREFIX} Stop Polling Queue Listing Request`);

export const deleteQueueListingRequest = createAction(`${QUEUE_PREFIX} Delete Queue Listing Request`);

export const deleteQueueListingRequestSuccess = createAction(`${QUEUE_PREFIX} Delete Queue Listing Request Success`);

export const viewFlowFile = createAction(`${QUEUE_PREFIX} View FlowFile`, props<{ request: ViewFlowFileRequest }>());

export const openFlowFileDialog = createAction(
    `${QUEUE_PREFIX} Open FlowFile Dialog`,
    props<{ request: FlowFileDialogRequest }>()
);

export const downloadFlowFileContent = createAction(
    `${QUEUE_PREFIX} Download FlowFile Content`,
    props<{ request: DownloadFlowFileContentRequest }>()
);

export const viewFlowFileContent = createAction(
    `${QUEUE_PREFIX} View FlowFile Content`,
    props<{ request: ViewFlowFileContentRequest }>()
);
