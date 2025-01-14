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
import { ErrorState } from './index';
import {
    resetErrorState,
    fullScreenError,
    addBannerError,
    clearBannerErrors,
    setRoutedToFullScreenError
} from './error.actions';
import { produce } from 'immer';

export const initialState: ErrorState = {
    bannerErrors: {},
    fullScreenError: null,
    routedToFullScreenError: false
};

export const errorReducer = createReducer(
    initialState,
    on(fullScreenError, (state, { errorDetail }) => ({
        ...state,
        fullScreenError: errorDetail
    })),
    on(addBannerError, (state, { errorContext }) => {
        return produce(state, (draftState) => {
            if (draftState.bannerErrors === null) {
                draftState.bannerErrors = {};
            }
            if (!draftState.bannerErrors[errorContext.context]) {
                draftState.bannerErrors[errorContext.context] = [];
            }

            draftState.bannerErrors[errorContext.context].push(...errorContext.errors);
        });
    }),
    on(clearBannerErrors, (state, { context }) => {
        return produce(state, (draftState) => {
            if (draftState.bannerErrors && draftState.bannerErrors[context]) {
                delete draftState.bannerErrors[context];
            }
        });
    }),
    on(setRoutedToFullScreenError, (state, { routedToFullScreenError }) => ({
        ...state,
        routedToFullScreenError
    })),
    on(resetErrorState, () => ({
        ...initialState
    }))
);
