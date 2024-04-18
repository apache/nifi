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
    ConfigureFlowAnalysisRuleRequest,
    ConfigureFlowAnalysisRuleSuccess,
    CreateFlowAnalysisRuleRequest,
    CreateFlowAnalysisRuleSuccess,
    DeleteFlowAnalysisRuleRequest,
    DeleteFlowAnalysisRuleSuccess,
    DisableFlowAnalysisRuleRequest,
    DisableFlowAnalysisRuleSuccess,
    EditFlowAnalysisRuleDialogRequest,
    EnableFlowAnalysisRuleRequest,
    EnableFlowAnalysisRuleSuccess,
    LoadFlowAnalysisRulesResponse,
    SelectFlowAnalysisRuleRequest
} from './index';
import { FetchComponentVersionsRequest } from '../../../../state/shared';

export const resetFlowAnalysisRulesState = createAction('[Flow Analysis Rules] Reset Flow Analysis Rules State');

export const loadFlowAnalysisRules = createAction('[Flow Analysis Rules] Load Flow Analysis Rules');

export const loadFlowAnalysisRulesSuccess = createAction(
    '[Flow Analysis Rules] Load Flow Analysis Rules Success',
    props<{ response: LoadFlowAnalysisRulesResponse }>()
);

export const openConfigureFlowAnalysisRuleDialog = createAction(
    '[Flow Analysis Rules] Open Flow Analysis Rule Dialog',
    props<{ request: EditFlowAnalysisRuleDialogRequest }>()
);

export const configureFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Configure Flow Analysis Rule',
    props<{ request: ConfigureFlowAnalysisRuleRequest }>()
);

export const configureFlowAnalysisRuleSuccess = createAction(
    '[Flow Analysis Rules] Configure Flow Analysis Rule Success',
    props<{ response: ConfigureFlowAnalysisRuleSuccess }>()
);

export const enableFlowAnalysisRule = createAction(
    '[Enable Flow Analysis Rule] Submit Enable Request',
    props<{
        request: EnableFlowAnalysisRuleRequest;
    }>()
);

export const enableFlowAnalysisRuleSuccess = createAction(
    '[Flow Analysis Rules] Enable Flow Analysis Rule Success',
    props<{ response: EnableFlowAnalysisRuleSuccess }>()
);

export const disableFlowAnalysisRule = createAction(
    '[Enable Flow Analysis Rule] Submit Disable Request',
    props<{
        request: DisableFlowAnalysisRuleRequest;
    }>()
);

export const disableFlowAnalysisRuleSuccess = createAction(
    '[Flow Analysis Rules] Disable Flow Analysis Rule Success',
    props<{ response: DisableFlowAnalysisRuleSuccess }>()
);

export const flowAnalysisRuleBannerApiError = createAction(
    '[Flow Analysis Rules] Load Flow Analysis Rule Banner Error',
    props<{ error: string }>()
);

export const flowAnalysisRuleSnackbarApiError = createAction(
    '[Flow Analysis Rules] Load Flow Analysis Rule Snackbar Error',
    props<{ error: string }>()
);

export const openNewFlowAnalysisRuleDialog = createAction('[Flow Analysis Rules] Open New Flow Analysis Rule Dialog');

export const createFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Create Flow Analysis Rule',
    props<{ request: CreateFlowAnalysisRuleRequest }>()
);

export const createFlowAnalysisRuleSuccess = createAction(
    '[Flow Analysis Rules] Create Flow Analysis Rule Success',
    props<{ response: CreateFlowAnalysisRuleSuccess }>()
);

export const navigateToEditFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Navigate To Edit Flow Analysis Rule',
    props<{ id: string }>()
);

export const promptFlowAnalysisRuleDeletion = createAction(
    '[Flow Analysis Rules] Prompt Flow Analysis Rule Deletion',
    props<{ request: DeleteFlowAnalysisRuleRequest }>()
);

export const deleteFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Delete Flow Analysis Rule',
    props<{ request: DeleteFlowAnalysisRuleRequest }>()
);

export const deleteFlowAnalysisRuleSuccess = createAction(
    '[Flow Analysis Rules] Delete Flow Analysis Rule Success',
    props<{ response: DeleteFlowAnalysisRuleSuccess }>()
);

export const selectFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Select Flow Analysis Rule',
    props<{ request: SelectFlowAnalysisRuleRequest }>()
);

export const openChangeFlowAnalysisRuleVersionDialog = createAction(
    `[Flow Analysis Rules] Open Change Flow Analysis Rule Version Dialog`,
    props<{ request: FetchComponentVersionsRequest }>()
);
