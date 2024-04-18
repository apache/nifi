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
    ConfigureControllerServiceRequest,
    ConfigureControllerServiceSuccess,
    CreateControllerServiceSuccess,
    DeleteControllerServiceRequest,
    DeleteControllerServiceSuccess,
    LoadManagementControllerServicesResponse,
    SelectControllerServiceRequest
} from './index';
import {
    CreateControllerServiceRequest,
    DisableControllerServiceDialogRequest,
    EditControllerServiceDialogRequest,
    FetchComponentVersionsRequest,
    SetEnableControllerServiceDialogRequest
} from '../../../../state/shared';

export const resetManagementControllerServicesState = createAction(
    '[Management Controller Services] Reset Management Controller Services State'
);

export const loadManagementControllerServices = createAction(
    '[Management Controller Services] Load Management Controller Services'
);

export const loadManagementControllerServicesSuccess = createAction(
    '[Management Controller Services] Load Management Controller Services Success',
    props<{ response: LoadManagementControllerServicesResponse }>()
);

export const managementControllerServicesBannerApiError = createAction(
    '[Management Controller Services] Management Controller Services Banner Api Error',
    props<{ error: string }>()
);

export const managementControllerServicesSnackbarApiError = createAction(
    '[Management Controller Services] Management Controller Services Snackbar Api Error',
    props<{ error: string }>()
);

export const openNewControllerServiceDialog = createAction(
    '[Management Controller Services] Open New Controller Service Dialog'
);

export const createControllerService = createAction(
    '[Management Controller Services] Create Controller Service',
    props<{ request: CreateControllerServiceRequest }>()
);

export const createControllerServiceSuccess = createAction(
    '[Management Controller Services] Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const inlineCreateControllerServiceSuccess = createAction(
    '[Management Controller Services] InlineCreate Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const navigateToEditService = createAction(
    '[Management Controller Services] Navigate To Edit Service',
    props<{ id: string }>()
);

export const navigateToAdvancedServiceUi = createAction(
    '[Controller Services] Navigate To Advanced Service UI',
    props<{ id: string }>()
);

export const openConfigureControllerServiceDialog = createAction(
    '[Management Controller Services] Open Configure Controller Service Dialog',
    props<{ request: EditControllerServiceDialogRequest }>()
);

export const configureControllerService = createAction(
    '[Management Controller Services] Configure Controller Service',
    props<{ request: ConfigureControllerServiceRequest }>()
);

export const configureControllerServiceSuccess = createAction(
    '[Management Controller Services] Configure Controller Service Success',
    props<{ response: ConfigureControllerServiceSuccess }>()
);

export const openEnableControllerServiceDialog = createAction(
    '[Management Controller Services] Open Enable Controller Service Dialog',
    props<{ request: SetEnableControllerServiceDialogRequest }>()
);

export const openDisableControllerServiceDialog = createAction(
    '[Management Controller Services] Open Disable Controller Service Dialog',
    props<{ request: DisableControllerServiceDialogRequest }>()
);

export const promptControllerServiceDeletion = createAction(
    '[Management Controller Services] Prompt Controller Service Deletion',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteControllerService = createAction(
    '[Management Controller Services] Delete Controller Service',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteControllerServiceSuccess = createAction(
    '[Management Controller Services] Delete Controller Service Success',
    props<{ response: DeleteControllerServiceSuccess }>()
);

export const selectControllerService = createAction(
    '[Management Controller Services] Select Controller Service',
    props<{ request: SelectControllerServiceRequest }>()
);

export const openChangeMgtControllerServiceVersionDialog = createAction(
    `[Management Controller Services] Open Change Management Controller Service Version Dialog`,
    props<{ request: FetchComponentVersionsRequest }>()
);
