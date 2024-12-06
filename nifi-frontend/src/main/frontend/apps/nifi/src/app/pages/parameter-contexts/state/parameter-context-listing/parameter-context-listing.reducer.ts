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
import { ParameterContextListingState } from './index';
import { produce } from 'immer';
import {
    createParameterContext,
    createParameterContextSuccess,
    deleteParameterContextSuccess,
    editParameterContextComplete,
    loadParameterContexts,
    loadParameterContextsSuccess,
    parameterContextListingSnackbarApiError,
    parameterContextListingBannerApiError,
    pollParameterContextUpdateRequestSuccess,
    submitParameterContextUpdateRequest,
    submitParameterContextUpdateRequestSuccess,
    deleteParameterContextUpdateRequestSuccess
} from './parameter-context-listing.actions';
import { ParameterContextUpdateRequestEntity } from '../../../../state/shared';
import { Revision } from '@nifi/shared';

export const initialState: ParameterContextListingState = {
    parameterContexts: [],
    updateRequestEntity: null,
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const parameterContextListingReducer = createReducer(
    initialState,
    on(loadParameterContexts, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadParameterContextsSuccess, (state, { response }) => ({
        ...state,
        parameterContexts: response.parameterContexts,
        loadedTimestamp: response.loadedTimestamp,
        error: null,
        status: 'success' as const
    })),
    on(parameterContextListingSnackbarApiError, parameterContextListingBannerApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(createParameterContext, (state) => ({
        ...state,
        saving: true
    })),
    on(createParameterContextSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.parameterContexts.push(response.parameterContext);
            draftState.saving = false;
        });
    }),
    on(submitParameterContextUpdateRequest, (state) => ({
        ...state,
        saving: true
    })),
    on(
        submitParameterContextUpdateRequestSuccess,
        pollParameterContextUpdateRequestSuccess,
        deleteParameterContextUpdateRequestSuccess,
        (state, { response }) => ({
            ...state,
            updateRequestEntity: response.requestEntity
        })
    ),
    on(deleteParameterContextUpdateRequestSuccess, (state) => ({
        ...state,
        saving: false
    })),
    on(editParameterContextComplete, (state) => {
        return produce(state, (draftState) => {
            const updateRequestEntity: ParameterContextUpdateRequestEntity | null = draftState.updateRequestEntity;

            if (updateRequestEntity) {
                const revision: Revision = updateRequestEntity.parameterContextRevision;
                const parameterContext: any = updateRequestEntity.request.parameterContext;

                const componentIndex: number = draftState.parameterContexts.findIndex(
                    (f: any) => parameterContext.id === f.id
                );
                if (componentIndex > -1) {
                    draftState.parameterContexts[componentIndex] = {
                        ...draftState.parameterContexts[componentIndex],
                        revision: {
                            ...revision
                        },
                        component: {
                            ...parameterContext
                        }
                    };
                }

                draftState.updateRequestEntity = null;
                draftState.saving = false;
            }
        });
    }),
    on(deleteParameterContextSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.parameterContexts.findIndex(
                (f: any) => response.parameterContext.id === f.id
            );
            if (componentIndex > -1) {
                draftState.parameterContexts.splice(componentIndex, 1);
            }
        });
    })
);
