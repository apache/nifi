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

import { ProvenanceEventSummary } from '../../../../state/shared';
import { NodeSearchResult } from '../../../../state/cluster-summary';

export const provenanceEventListingFeatureKey = 'provenanceEventListing';

export interface ProvenanceOptionsResponse {
    provenanceOptions: ProvenanceOptions;
}

export interface ProvenanceQueryParams {
    flowFileUuid?: string;
    componentId?: string;
}

export interface ProvenanceQueryResponse {
    provenance: Provenance;
}

export interface ProvenanceEventRequest {
    eventId: number;
    clusterNodeId?: string;
}

export interface GoToProvenanceEventSourceRequest {
    eventId?: number;
    componentId?: string;
    groupId?: string;
    clusterNodeId?: string;
}

export interface SearchableField {
    field: string;
    id: string;
    label: string;
    type: string;
}

export interface ProvenanceOptions {
    searchableFields: SearchableField[];
}

export interface OpenSearchRequest {
    clusterNodes: NodeSearchResult[];
}

export interface ProvenanceSearchDialogRequest {
    timeOffset: number;
    clusterNodes: NodeSearchResult[];
    options: ProvenanceOptions;
    currentRequest: ProvenanceRequest;
}

export interface ProvenanceSearchValue {
    value: string;
    inverse: boolean;
}

export interface ProvenanceRequest {
    searchTerms?: {
        [key: string]: ProvenanceSearchValue;
    };
    clusterNodeId?: string;
    startDate?: string;
    endDate?: string;
    minimumFileSize?: string;
    maximumFileSize?: string;
    maxResults: number;
    summarize: boolean;
    incrementalResults: boolean;
}

export interface ProvenanceResults {
    provenanceEvents: ProvenanceEventSummary[];
    total: string;
    totalCount: number;
    generated: string;
    oldestEvent: string;
    timeOffset: number;
    errors: string[];
}

export interface Provenance {
    id: string;
    uri: string;
    submissionTime: string;
    expiration: string;
    percentCompleted: number;
    finished: boolean;
    request: ProvenanceRequest;
    results: ProvenanceResults;
}

export interface ProvenanceEventListingState {
    options: ProvenanceOptions | null;
    request: ProvenanceRequest | null;
    activeProvenance: Provenance | null;
    completedProvenance: Provenance;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'error' | 'success';
}
