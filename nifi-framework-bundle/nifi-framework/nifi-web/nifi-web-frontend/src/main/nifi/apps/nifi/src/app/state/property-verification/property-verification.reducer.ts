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

import { createReducer, on } from '@ngrx/store';
import { PropertyVerificationState } from './index';
import {
    getConfigurationAnalysisSuccess,
    initiatePropertyVerification,
    pollPropertyVerificationSuccess,
    propertyVerificationComplete,
    resetPropertyVerificationState,
    verifyPropertiesSuccess
} from './property-verification.actions';
import { produce } from 'immer';

export const initialPropertyVerificationState: PropertyVerificationState = {
    results: [],
    status: 'pending',
    requestContext: null,
    activeRequest: null,
    configurationAnalysis: null,
    attributes: null
};

export const propertyVerificationReducer = createReducer(
    initialPropertyVerificationState,

    on(initiatePropertyVerification, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.status = 'loading';
            draftState.configurationAnalysis = response.configurationAnalysis;
            draftState.requestContext = response.requestContext;
            if (response.configurationAnalysis.supportsVerification) {
                // preserve the most recent attributes used to verify component
                draftState.attributes = response.configurationAnalysis.referencedAttributes;
            }
        });
    }),
    on(getConfigurationAnalysisSuccess, (state: PropertyVerificationState, { response }) => ({
        ...state,
        configurationAnalysis: response.configurationAnalysis,
        requestContext: response.requestContext
    })),

    on(verifyPropertiesSuccess, pollPropertyVerificationSuccess, (state: PropertyVerificationState, { response }) => ({
        ...state,
        activeRequest: response.request,
        results: response.request.results || []
    })),

    on(propertyVerificationComplete, (state) => ({
        ...state,
        activeRequest: null,
        status: 'success' as const
    })),

    on(resetPropertyVerificationState, (state) => ({
        ...initialPropertyVerificationState,
        attributes: state.attributes // preserve attributes
    }))
);
