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

import { JoltTransformJsonUiState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    loadProcessorDetailsFailure,
    loadProcessorDetailsSuccess,
    resetJoltTransformJsonUiState,
    resetSavePropertiesState,
    resetValidateJoltSpecState,
    saveProperties,
    savePropertiesFailure,
    savePropertiesSuccess,
    transformJoltSpec,
    transformJoltSpecFailure,
    transformJoltSpecSuccess,
    validateJoltSpec,
    validateJoltSpecFailure,
    validateJoltSpecSuccess
} from './jolt-transform-json-ui.actions';

export const initialState: JoltTransformJsonUiState = {
    saving: false,
    loadedTimestamp: '',
    status: 'pending',
    processorDetails: null,
    transformingJoltSpec: false,
    validatingJoltSpec: false,
    savingProperties: false
};

export const joltTransformJsonUiReducer = createReducer(
    initialState,
    on(resetJoltTransformJsonUiState, () => ({
        ...initialState
    })),
    on(loadProcessorDetailsSuccess, (state, { response }) => ({
        ...state,
        processorDetails: response
    })),
    on(loadProcessorDetailsFailure, (state) => ({
        ...state,
        processorDetails: null
    })),
    on(saveProperties, (state) => ({
        ...state,
        savingProperties: true,
        validatingJoltSpec: null,
        validationResponse: null,
        validationFailureResponse: null
    })),
    on(savePropertiesSuccess, (state, { response }) => ({
        ...state,
        savingProperties: false,
        savePropertiesResponse: response
    })),
    on(savePropertiesFailure, (state, { response }) => ({
        ...state,
        savingProperties: false,
        savePropertiesFailureResponse: response
    })),
    on(resetSavePropertiesState, (state) => ({
        ...state,
        savingProperties: false,
        savePropertiesResponse: null,
        savePropertiesFailureResponse: null
    })),
    on(validateJoltSpec, (state) => ({
        ...state,
        validatingJoltSpec: true
    })),
    on(validateJoltSpecSuccess, (state, { response }) => ({
        ...state,
        validatingJoltSpec: false,
        validationResponse: response,
        savingProperties: false,
        savePropertiesResponse: null,
        savePropertiesFailureResponse: null
    })),
    on(validateJoltSpecFailure, (state, { response }) => ({
        ...state,
        validatingJoltSpec: false,
        validationFailureResponse: response
    })),
    on(resetValidateJoltSpecState, (state) => ({
        ...state,
        validatingJoltSpec: null,
        validationResponse: null,
        validationFailureResponse: null
    })),
    on(transformJoltSpec, (state) => ({
        ...state,
        transformingJoltSpec: true
    })),
    on(transformJoltSpecSuccess, (state, { response }) => ({
        ...state,
        transformingJoltSpec: false,
        transformationResponse: response
    })),
    on(transformJoltSpecFailure, (state, { response }) => ({
        ...state,
        transformingJoltSpec: false,
        transformationResponse: null,
        transformationFailureResponse: response
    }))
);
