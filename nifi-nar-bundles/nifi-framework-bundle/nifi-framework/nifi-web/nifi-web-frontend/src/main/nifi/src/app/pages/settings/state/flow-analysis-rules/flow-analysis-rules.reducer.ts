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
import { FlowAnalysisRulesState } from './index';
import {
    configureFlowAnalysisRule,
    configureFlowAnalysisRuleSuccess,
    createFlowAnalysisRule,
    createFlowAnalysisRuleSuccess,
    deleteFlowAnalysisRule,
    deleteFlowAnalysisRuleSuccess,
    loadFlowAnalysisRules,
    loadFlowAnalysisRulesSuccess,
    flowAnalysisRuleBannerApiError,
    resetFlowAnalysisRulesState,
    disableFlowAnalysisRule,
    enableFlowAnalysisRule,
    enableFlowAnalysisRuleSuccess,
    disableFlowAnalysisRuleSuccess,
    flowAnalysisRuleSnackbarApiError
} from './flow-analysis-rules.actions';
import { produce } from 'immer';

export const initialState: FlowAnalysisRulesState = {
    flowAnalysisRules: [],
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const flowAnalysisRulesReducer = createReducer(
    initialState,
    on(resetFlowAnalysisRulesState, () => ({
        ...initialState
    })),
    on(loadFlowAnalysisRules, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadFlowAnalysisRulesSuccess, (state, { response }) => ({
        ...state,
        flowAnalysisRules: response.flowAnalysisRules,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const
    })),
    on(flowAnalysisRuleBannerApiError, flowAnalysisRuleSnackbarApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(enableFlowAnalysisRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flowAnalysisRules.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.flowAnalysisRules[componentIndex] = response.flowAnalysisRule;
            }
            draftState.saving = false;
        });
    }),
    on(disableFlowAnalysisRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flowAnalysisRules.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.flowAnalysisRules[componentIndex] = response.flowAnalysisRule;
            }
            draftState.saving = false;
        });
    }),
    on(configureFlowAnalysisRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flowAnalysisRules.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.flowAnalysisRules[componentIndex] = response.flowAnalysisRule;
            }
            draftState.saving = false;
        });
    }),
    on(
        createFlowAnalysisRule,
        deleteFlowAnalysisRule,
        configureFlowAnalysisRule,
        enableFlowAnalysisRule,
        disableFlowAnalysisRule,
        (state) => ({
            ...state,
            saving: true
        })
    ),
    on(createFlowAnalysisRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.flowAnalysisRules.push(response.flowAnalysisRule);
            draftState.saving = false;
        });
    }),
    on(deleteFlowAnalysisRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flowAnalysisRules.findIndex(
                (f: any) => response.flowAnalysisRule.id === f.id
            );
            if (componentIndex > -1) {
                draftState.flowAnalysisRules.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    })
);
