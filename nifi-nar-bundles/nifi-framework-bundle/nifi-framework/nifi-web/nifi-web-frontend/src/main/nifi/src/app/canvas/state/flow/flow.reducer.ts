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
  addSelectedComponents,
  loadFlow,
  loadFlowFailure,
  loadFlowSuccess, removeSelectedComponents,
  setSelectedComponents, setTransition,
  updatePositionSuccess
} from './flow.actions';
import { FlowState } from '../index';
import { produce } from 'immer';

export const initialState: FlowState = {
  flow: {
    permissions: {
      canRead: false,
      canWrite: false
    },
    processGroupFlow: {
      id: '',
      uri: '',
      parentGroupId: '',
      breadcrumb: {},
      flow: {
        processGroups: [],
        remoteProcessGroups: [],
        processors: [],
        inputPorts: [],
        outputPorts: [],
        connections: [],
        labels: [],
        funnels: []
      },
      lastRefreshed: ''
    }
  },
  selection: [],
  transition: false,
  error: null,
  status: 'pending'
}

export const flowReducer = createReducer(
  initialState,
  on(loadFlow, (state) => ({
    ...state,
    transition: true,
    status: 'loading' as const
  })),
  on(loadFlowSuccess, (state, { flow }) => ({
    ...state,
    flow: flow,
    error: null,
    status: 'success' as const
  })),
  on(loadFlowFailure, (state, { error }) => ({
    ...state,
    error: error,
    status: 'error' as const
  })),
  on(addSelectedComponents, (state, { ids }) => ({
    ...state,
    selection: [...state.selection, ...ids],
  })),
  on(setSelectedComponents, (state, { ids }) => ({
    ...state,
    selection: ids,
  })),
  on(removeSelectedComponents, (state, { ids }) => ({
    ...state,
    selection: state.selection.filter(id => !ids.includes(id)),
  })),
  on(updatePositionSuccess, (state, { positionUpdates }) => {
    return produce(state, draftState => {
      positionUpdates.componentPositionUpdates.forEach(positionUpdate => {
        const funnel = draftState.flow.processGroupFlow.flow.funnels.find((f: any) => positionUpdate.id === f.id);
        if (funnel) {
          funnel.position = positionUpdate.position;
          funnel.component.position = positionUpdate.position;
        }
      })
    });
  }),
  on(setTransition, (state, { transition }) => ({
    ...state,
    transition: transition,
  }))
);
