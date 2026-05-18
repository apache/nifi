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
import { ProvenanceEvent, ProvenanceEventDialogRequest } from '../../../../state/shared';
import { DownloadContentRequest, ReplayEventRequest, ViewContentRequest } from './index';

export const loadLatestEventsForComponent = createAction(
    '[Connector Provenance] Load Latest Events For Component',
    props<{ componentId: string }>()
);

export const loadLatestEventsForComponentSuccess = createAction(
    '[Connector Provenance] Load Latest Events For Component Success',
    props<{ events: ProvenanceEvent[] }>()
);

export const loadError = createAction('[Connector Provenance] Load Error', props<{ error: string }>());

export const resetState = createAction('[Connector Provenance] Reset State');

export const openProvenanceEventDialog = createAction(
    '[Connector Provenance] Open Provenance Event Dialog',
    props<{ request: ProvenanceEventDialogRequest }>()
);

export const downloadContent = createAction(
    '[Connector Provenance] Download Content',
    props<{ request: DownloadContentRequest }>()
);

export const viewContent = createAction(
    '[Connector Provenance] View Content',
    props<{ request: ViewContentRequest }>()
);

export const replayEvent = createAction(
    '[Connector Provenance] Replay Event',
    props<{ request: ReplayEventRequest }>()
);

export const showOkDialog = createAction(
    '[Connector Provenance] Show Ok Dialog',
    props<{ title: string; message: string }>()
);
