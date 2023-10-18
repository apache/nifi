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
    ConfigureControllerService,
    ConfigureControllerServiceSuccess,
    CreateControllerService,
    CreateControllerServiceSuccess,
    DeleteControllerService,
    DeleteControllerServiceSuccess,
    LoadManagementControllerServicesResponse
} from './index';
import { EditControllerServiceRequest } from '../../../../state/shared';

export const loadManagementControllerServices = createAction(
    '[Management Controller Services] Load Management Controller Services'
);

export const loadManagementControllerServicesSuccess = createAction(
    '[Management Controller Services] Load Management Controller Services Success',
    props<{ response: LoadManagementControllerServicesResponse }>()
);

export const managementControllerServicesApiError = createAction(
    '[Management Controller Services] Load Management Controller Services Error',
    props<{ error: string }>()
);

export const openNewControllerServiceDialog = createAction(
    '[Management Controller Services] Open New Controller Service Dialog'
);

export const createControllerService = createAction(
    '[Management Controller Services] Create Controller Service',
    props<{ request: CreateControllerService }>()
);

export const createControllerServiceSuccess = createAction(
    '[Management Controller Services] Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const openConfigureControllerServiceDialog = createAction(
    '[Management Controller Services] Open Configure Controller Service Dialog',
    props<{ request: EditControllerServiceRequest }>()
);

export const configureControllerService = createAction(
    '[Management Controller Services] Configure Controller Service',
    props<{ request: ConfigureControllerService }>()
);

export const configureControllerServiceSuccess = createAction(
    '[Management Controller Services] Configure Controller Service Success',
    props<{ response: ConfigureControllerServiceSuccess }>()
);

export const promptControllerServiceDeletion = createAction(
    '[Management Controller Services] Prompt Controller Service Deletion',
    props<{ request: DeleteControllerService }>()
);

export const deleteControllerService = createAction(
    '[Management Controller Services] Delete Controller Service',
    props<{ request: DeleteControllerService }>()
);

export const deleteControllerServiceSuccess = createAction(
    '[Management Controller Services] Delete Controller Service Success',
    props<{ response: DeleteControllerServiceSuccess }>()
);
