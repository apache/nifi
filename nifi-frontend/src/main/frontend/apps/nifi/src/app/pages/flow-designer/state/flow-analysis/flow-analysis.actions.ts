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
import { FlowAnalysisRequestResponse, FlowAnalysisRule, FlowAnalysisRuleViolation } from '.';

export const startPollingFlowAnalysis = createAction('[Flow Analysis] Start Polling Flow Analysis');

export const stopPollingFlowAnalysis = createAction('[Flow Analysis] Stop Polling Flow Analysis');

export const pollFlowAnalysis = createAction(`[Flow Analysis] Poll Flow Analysis`);

export const pollFlowAnalysisSuccess = createAction(
    `[Flow Analysis] Poll Flow Analysis Success`,
    props<{ response: FlowAnalysisRequestResponse }>()
);

export const flowAnalysisApiError = createAction('[Flow Analysis] API Error', props<{ error: string }>());

export const resetPollingFlowAnalysis = createAction(`[Flow Analysis] Reset Polling Flow Analysis`);

export const navigateToEditFlowAnalysisRule = createAction(
    '[Flow Analysis Rules] Navigate To Edit Flow Analysis Rule',
    props<{ id: string }>()
);

export const openRuleDetailsDialog = createAction(
    '[Flow Analysis Rules] Open Flow Analysis Rule Details Dialog',
    props<{ violation: FlowAnalysisRuleViolation; rule: FlowAnalysisRule }>()
);
