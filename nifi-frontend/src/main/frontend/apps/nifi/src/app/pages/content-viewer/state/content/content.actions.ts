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
import { loadSideBarDataRequestSuccess } from './index';
import { DownloadFlowFileContentRequest } from '../../../queue/state/queue-listing';

export const setRef = createAction('[Content] Set Ref', props<{ ref: string }>());

export const navigateToBundledContentViewer = createAction(
    '[Content] Navigate To Bundled Content Viewer',
    props<{ route: string }>()
);

export const resetContent = createAction('[Content] Reset Content');

export const loadSideBarData = createAction('[Content] Load SideBar Data');

export const loadSideBarDataSuccess = createAction(
    '[Content] Load SideBar Data Success',
    props<{ response: loadSideBarDataRequestSuccess }>()
);

export const downloadContentWithFlowFile = createAction(
    '[Content] Download Content With FlowFile',
    props<{ request: DownloadFlowFileContentRequest }>()
);

export const downloadContentWithEvent = createAction(
    '[Content] Download Content With Event',
    props<{ eventId: number; direction: string; clusterNodeId?: string }>()
);
