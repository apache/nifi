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
import { DeleteDropletRequest, Droplets, ImportDropletDialog, ImportDropletRequest, LoadDropletsResponse } from '.';

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

export const deleteDropletSuccess = createAction('[Droplets] Delete Droplet Success', props<{ response: Droplets }>());

export const openImportNewFlowDialog = createAction(
    '[Droplets] Open Import New Flow Dialog',
    props<{ request: ImportDropletDialog }>()
);

export const createNewFlow = createAction('[Droplets] Create New Flow', props<{ request: ImportDropletRequest }>());

export const importNewFlow = createAction(
    '[Droplets] Import New Flow',
    props<{ href: string; request: ImportDropletRequest }>()
);

export const importNewFlowSuccess = createAction('[Droplets] Import New Flow Success', props<{ response: any }>());

export const openImportNewFlowVersionDialog = createAction(
    '[Droplets] Open Import New Flow Version Dialog',
    props<{ request: { droplet: Droplets } }>()
);

export const importNewFlowVersion = createAction(
    '[Droplets] Import New Flow Version',
    props<{ request: ImportDropletRequest; flowId: string }>()
);

export const openExportFlowVersionDialog = createAction(
    `[Droplets] Open Export Flow Version Dialog`,
    props<{ request: { droplet: Droplets } }>()
);

export const exportFlowVersion = createAction(
    `[Droplets] Export Flow Version`,
    props<{ request: { droplet: Droplets; version: number } }>()
);

export const exportFlowVersionSuccess = createAction(
    `[Droplets] Export Flow Version Success`,
    props<{ response: any }>()
);

export const openFlowVersionsDialog = createAction(
    `[Droplets] Open Flow Versions Dialog`,
    props<{ request: { droplet: Droplets } }>()
);
