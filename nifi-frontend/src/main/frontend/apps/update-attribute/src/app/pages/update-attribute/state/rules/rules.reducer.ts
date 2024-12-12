/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { RulesState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    createRule,
    createRuleSuccess,
    deleteRule,
    deleteRuleSuccess,
    editRule,
    editRuleSuccess,
    loadRules,
    loadRulesFailure,
    loadRulesSuccess,
    populateNewRule,
    resetNewRule,
    resetRulesState,
    saveRuleFailure
} from './rules.actions';
import { produce } from 'immer';

export const initialState: RulesState = {
    loading: false,
    saving: false,
    error: null,
    rules: null
};

export const rulesReducer = createReducer(
    initialState,
    on(resetRulesState, () => ({
        ...initialState
    })),
    on(loadRules, (state) => ({
        ...state,
        error: null,
        loading: true
    })),
    on(loadRulesSuccess, (state, { rulesEntity }) => ({
        ...state,
        loading: false,
        rules: rulesEntity.rules
    })),
    on(loadRulesFailure, (state, { error }) => ({
        ...state,
        loading: false,
        error
    })),
    on(populateNewRule, (state, { rule }) => ({
        ...state,
        newRule: {
            name: rule ? `Copy of ${rule.name}` : '',
            comments: rule ? rule.comments : '',
            conditions: rule ? rule.conditions : [],
            actions: rule ? rule.actions : []
        }
    })),
    on(resetNewRule, (state) => ({
        ...state,
        newRule: undefined
    })),
    on(createRule, editRule, deleteRule, (state) => ({
        ...state,
        saving: true
    })),
    on(saveRuleFailure, (state) => ({
        ...state,
        saving: false
    })),
    on(createRuleSuccess, (state, { entity }) => {
        return produce(state, (draftState) => {
            if (draftState.rules) {
                draftState.rules.push(entity.rule);
            }
            draftState.saving = false;
            draftState.newRule = undefined;
        });
    }),
    on(editRuleSuccess, (state, { entity }) => {
        return produce(state, (draftState) => {
            if (draftState.rules) {
                const componentIndex: number = draftState.rules.findIndex((r: any) => entity.rule.id === r.id);
                if (componentIndex > -1) {
                    draftState.rules[componentIndex] = {
                        ...entity.rule
                    };
                }
            }
            draftState.saving = false;
        });
    }),
    on(deleteRuleSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            if (draftState.rules) {
                const componentIndex: number = draftState.rules.findIndex((r: any) => response.id === r.id);
                if (componentIndex > -1) {
                    draftState.rules.splice(componentIndex, 1);
                }
            }
            draftState.saving = false;
        });
    })
);
