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

import { createReducer, on } from '@ngrx/store';
import { FlowAnalysisState } from './index';
import { pollFlowAnalysis, pollFlowAnalysisSuccess } from './flow-analysis.actions';

export const initialState: FlowAnalysisState = {
    rules: [],
    ruleViolations: [],
    flowAnalysisPending: false,
    status: 'pending'
};

export const flowAnalysisReducer = createReducer(
    initialState,
    on(pollFlowAnalysis, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(pollFlowAnalysisSuccess, (state, { response }) => ({
        ...state,
        rules: response.rules,
        ruleViolations: response.ruleViolations,
        flowAnalysisPending: response.flowAnalysisPending,
        status: 'success' as const
    }))
);
