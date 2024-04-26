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

export const componentStateFeatureKey = 'componentState';

export interface ComponentStateRequest {
    componentName: string;
    componentUri: string;
    canClear: boolean;
}

export interface LoadComponentStateRequest {
    componentUri: string;
}

export interface ClearComponentStateRequest {
    componentUri: string;
}

export interface ComponentStateResponse {
    componentState: ComponentState;
}

export interface StateEntry {
    key: string;
    value: string;
    clusterNodeId?: string;
    clusterNodeAddress?: string;
}

export interface StateItem {
    key: string;
    value: string;
    scope?: string;
}

export interface StateMap {
    scope: string;
    state: StateEntry[];
    totalEntryCount: number;
}

export interface ComponentState {
    componentId: string;
    localState?: StateMap;
    clusterState?: StateMap;
    stateDescription: string;
}

export interface ComponentStateState {
    componentName: string | null;
    componentUri: string | null;
    componentState: ComponentState | null;
    canClear: boolean | null;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
