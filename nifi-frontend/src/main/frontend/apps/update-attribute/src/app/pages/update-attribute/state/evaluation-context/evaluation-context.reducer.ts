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

import { EvaluationContextState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    loadEvaluationContext,
    loadEvaluationContextFailure,
    loadEvaluationContextSuccess,
    resetEvaluationContextFailure,
    resetEvaluationContextState,
    saveEvaluationContext,
    saveEvaluationContextFailure,
    saveEvaluationContextSuccess
} from './evaluation-context.actions';

export const initialState: EvaluationContextState = {
    loading: false,
    saving: false,
    error: null,
    evaluationContext: null
};

export const evaluationContextReducer = createReducer(
    initialState,
    on(resetEvaluationContextState, () => ({
        ...initialState
    })),
    on(loadEvaluationContext, (state) => ({
        ...state,
        loading: true
    })),
    on(loadEvaluationContextSuccess, (state, { evaluationContextEntity }) => ({
        ...state,
        loading: false,
        evaluationContext: {
            ruleOrder: evaluationContextEntity.ruleOrder,
            flowFilePolicy: evaluationContextEntity.flowFilePolicy
        }
    })),
    on(loadEvaluationContextFailure, (state, { error }) => ({
        ...state,
        error,
        loading: false
    })),
    on(saveEvaluationContext, (state) => ({
        ...state,
        saving: true
    })),
    on(saveEvaluationContextSuccess, (state, { evaluationContextEntity }) => ({
        ...state,
        saving: false,
        evaluationContext: {
            ruleOrder: evaluationContextEntity.ruleOrder,
            flowFilePolicy: evaluationContextEntity.flowFilePolicy
        }
    })),
    on(saveEvaluationContextFailure, (state, { error }) => ({
        ...state,
        error,
        saving: false
    })),
    on(resetEvaluationContextFailure, (state) => ({
        ...state,
        error: null
    }))
);
