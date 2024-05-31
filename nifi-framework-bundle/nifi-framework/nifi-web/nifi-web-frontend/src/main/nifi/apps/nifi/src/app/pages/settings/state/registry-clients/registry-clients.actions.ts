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
    CreateRegistryClientRequest,
    CreateRegistryClientSuccess,
    DeleteRegistryClientRequest,
    DeleteRegistryClientSuccess,
    EditRegistryClientDialogRequest,
    EditRegistryClientRequest,
    EditRegistryClientRequestSuccess,
    LoadRegistryClientsResponse,
    SelectRegistryClientRequest
} from './index';

export const resetRegistryClientsState = createAction('[Registry Clients] Reset Registry Clients State');

export const loadRegistryClients = createAction('[Registry Clients] Load Registry Clients');

export const loadRegistryClientsSuccess = createAction(
    '[Registry Clients] Load Registry Clients Success',
    props<{ response: LoadRegistryClientsResponse }>()
);

export const registryClientsBannerApiError = createAction(
    '[Registry Clients] Load Registry Clients Banner Api Error',
    props<{ error: string }>()
);

export const registryClientsSnackbarApiError = createAction(
    '[Registry Clients] Load Registry Clients Snackbar Api Error',
    props<{ error: string }>()
);

export const openNewRegistryClientDialog = createAction('[Registry Clients] Open New Registry Client Dialog');

export const createRegistryClient = createAction(
    '[Registry Clients] Create Registry Client',
    props<{ request: CreateRegistryClientRequest }>()
);

export const createRegistryClientSuccess = createAction(
    '[Registry Clients] Create Registry Client Success',
    props<{ response: CreateRegistryClientSuccess }>()
);

export const navigateToEditRegistryClient = createAction(
    '[Registry Clients] Navigate To Edit Registry Client',
    props<{ id: string }>()
);

export const openConfigureRegistryClientDialog = createAction(
    '[Registry Clients] Open Configure Registry Client Dialog',
    props<{ request: EditRegistryClientDialogRequest }>()
);

export const configureRegistryClient = createAction(
    '[Registry Clients] Configure Registry Client',
    props<{ request: EditRegistryClientRequest }>()
);

export const configureRegistryClientSuccess = createAction(
    '[Registry Clients] Configure Registry Client Success',
    props<{ response: EditRegistryClientRequestSuccess }>()
);

export const promptRegistryClientDeletion = createAction(
    '[Registry Clients] Prompt Registry Client Deletion',
    props<{ request: DeleteRegistryClientRequest }>()
);

export const deleteRegistryClient = createAction(
    '[Registry Clients] Delete Registry Client',
    props<{ request: DeleteRegistryClientRequest }>()
);

export const deleteRegistryClientSuccess = createAction(
    '[Registry Clients] Delete Registry Client Success',
    props<{ response: DeleteRegistryClientSuccess }>()
);

export const selectClient = createAction(
    '[Registry Clients] Select Registry Client',
    props<{ request: SelectRegistryClientRequest }>()
);
