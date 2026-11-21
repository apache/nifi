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

import { ProcessGroupDrainRequest, ProcessGroupDrainRequestEntity } from '@nifi/shared';
import { DrainDialogStatus } from '../../../../ui/common/drain-process-group-dialog/drain-process-group-dialog.types';

export const processGroupDrainFeatureKey = 'processGroupDrain';

export interface SubmitDrainRequest {
    processGroupId: string;
}

export interface ViewDrainRequest {
    processGroupId: string;
    drainRequestId: string;
}

export interface DrainRequestSuccess {
    drainRequestEntity: ProcessGroupDrainRequestEntity;
}

export interface DrainProcessGroupState {
    processGroupId: string | null;
    drainRequestId: string | null;
    drainRequest: ProcessGroupDrainRequest | null;
    status: DrainDialogStatus;
    error: string | null;
}
