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

export const queueListingFeatureKey = 'queueListing';

export interface FlowFile extends FlowFileSummary {
    attributes: {
        [key: string]: string;
    };
    contentClaimContainer?: string;
    contentClaimSection?: string;
    contentClaimIdentifier?: string;
    contentClaimOffset?: number;
    contentClaimFileSize?: string;
    contentClaimFileSizeBytes?: number;
}

export interface FlowFileSummary {
    uri: string;
    uuid: string;
    filename: string;
    mimeType?: string;
    position?: number;
    size: number;
    queuedDuration: number;
    lineageDuration: number;
    penaltyExpiresIn: number;
    penalized: boolean;
    clusterNodeId?: string;
    clusterNodeAddress?: string;
}

export interface QueueSize {
    byteCount: number;
    objectCount: number;
}

export interface ListingRequest {
    id: string;
    uri: string;
    submissionTime: string;
    lastUpdated: string;
    percentCompleted: number;
    finished: boolean;
    failureReason: string;
    maxResults: number;
    sourceRunning: boolean;
    destinationRunning: boolean;
    state: string;
    queueSize: QueueSize;
    flowFileSummaries: FlowFileSummary[];
}

export interface ListingRequestEntity {
    listingRequest: ListingRequest;
}

export interface LoadConnectionLabelRequest {
    connectionId: string;
}

export interface LoadConnectionLabelResponse {
    connectionId: string;
    connectionLabel: string;
}

export interface SubmitQueueListingRequest {
    connectionId: string;
}

export interface PollQueueListingSuccess {
    requestEntity: ListingRequestEntity;
}

export interface ViewFlowFileRequest {
    flowfileSummary: FlowFileSummary;
}

export interface DownloadFlowFileContentRequest {
    uri: string;
    clusterNodeId?: string;
}

export interface ViewFlowFileContentRequest {
    uri: string;
    mimeType?: string;
    clusterNodeId?: string;
}

export interface FlowFileDialogRequest {
    flowfile: FlowFile;
    clusterNodeId?: string;
}

export interface SelectedConnection {
    id: string;
    label: string;
}

export interface QueueListingState {
    activeListingRequest: ListingRequest | null;
    completedListingRequest: ListingRequest;
    selectedConnection: SelectedConnection | null;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'error' | 'success';
}
