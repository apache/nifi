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
import {
    deleteDroplet,
    deleteDropletSuccess,
    importNewFlow,
    importNewFlowSuccess,
    loadDroplets,
    loadDropletsSuccess
} from './droplets.actions';
import { DropletsState } from '.';
import { produce } from 'immer';

export const initialState: DropletsState = {
    droplets: [],
    status: 'pending',
    saving: false
};

export const dropletsReducer = createReducer(
    initialState,
    on(loadDroplets, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadDropletsSuccess, (state, { response }) => ({
        ...state,
        droplets: response.droplets,
        status: 'success' as const
    })),
    on(deleteDroplet, (state) => ({
        ...state,
        saving: true
    })),
    on(deleteDropletSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.droplets.findIndex(
                (f: any) => response.identifier === f.identifier
            );
            if (componentIndex > -1) {
                draftState.droplets.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    }),
    on(importNewFlow, (state) => ({
        ...state,
        saving: true
    })),
    on(importNewFlowSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.droplets.findIndex(
                (f: any) => response.flow.identifier === f.identifier
            );
            if (componentIndex === -1) {
                draftState.droplets.push(response.flow);
            } else {
                draftState.droplets[componentIndex].versionCount++;
            }
            draftState.saving = false;
        });
    })
);
