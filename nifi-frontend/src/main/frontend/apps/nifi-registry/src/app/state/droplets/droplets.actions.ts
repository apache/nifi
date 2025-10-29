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
    DeleteDropletRequest,
    Droplet,
    ImportDropletDialog,
    ImportDropletRequest,
    ImportDropletVersionRequest,
    LoadDropletsResponse
} from '.';
import { ErrorContext } from '../error';

export const loadDroplets = createAction('[Droplets] Load Droplets');

export const loadDropletsSuccess = createAction(
    '[Droplets] Load Droplets User Success',
    props<{ response: LoadDropletsResponse }>()
);

export const openDeleteDropletDialog = createAction(
    '[Droplets] Open Delete Droplet Dialog',
    props<{ request: DeleteDropletRequest }>()
);

export const deleteDroplet = createAction('[Droplets] Delete Droplet', props<{ request: DeleteDropletRequest }>());

export const deleteDropletSuccess = createAction('[Droplets] Delete Droplet Success', props<{ response: Droplet }>());

export const deleteDropletFailure = createAction('[Droplets] Delete Droplet Failure');

export const openImportNewDropletDialog = createAction(
    '[Droplets] Open Import New Droplet Dialog',
    props<{ request: ImportDropletDialog }>()
);

export const createNewDroplet = createAction(
    '[Droplets] Create New Droplet',
    props<{ request: ImportDropletRequest }>()
);

export const importNewDropletVersion = createAction(
    '[Droplets] Import New Droplet Version',
    props<{ request: ImportDropletVersionRequest }>()
);

export const createNewDropletSuccess = createAction(
    `[Droplets] Create New Droplet Success`,
    props<{ response: Droplet; request: ImportDropletVersionRequest }>()
);

export const importNewDropletVersionSuccess = createAction(
    '[Droplets] Import New Droplet Version Success',
    props<{ response: any }>()
);

export const openImportNewDropletVersionDialog = createAction(
    '[Droplets] Open Import New Droplet Version Dialog',
    props<{ request: { droplet: Droplet } }>()
);

export const openExportDropletVersionDialog = createAction(
    `[Droplets] Open Export Droplet Version Dialog`,
    props<{ request: { droplet: Droplet } }>()
);

export const exportDropletVersion = createAction(
    `[Droplets] Export Droplet Version`,
    props<{ request: { droplet: Droplet; version: number } }>()
);

export const exportDropletVersionSuccess = createAction(
    `[Droplets] Export Droplet Version Success`,
    props<{ response: any }>()
);

export const openDropletVersionsDialog = createAction(
    `[Droplets] Open Droplet Versions Dialog`,
    props<{ request: { droplet: Droplet } }>()
);

export const dropletsBannerError = createAction(`[Droplets] Banner Error`, props<{ errorContext: ErrorContext }>());

export const importNewDropletVersionError = createAction(
    '[Droplets] Import New Droplet Version Error',
    props<{ errorContext: ErrorContext; createdDroplet: Droplet }>()
);

export const importVersionForNewDroplet = createAction(
    '[Droplets] Import Version For New Droplet',
    props<{ request: ImportDropletVersionRequest; createdDroplet: Droplet }>()
);

// Utility no-op action for effects that need to emit a valid action on success. This allows dialogs to add global
// context errors when the dialog is unable to async GET the data it needs in order to open.
export const noOp = createAction('[Droplets] No Op');
