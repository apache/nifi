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

export interface DropRequest {
    id: string;
    uri: string;
    submissionTime: string;
    lastUpdated: string;
    percentCompleted: number;
    finished: boolean;
    failureReason: string;
    currentCount: number;
    currentSize: number;
    current: string;
    originalCount: number;
    originalSize: number;
    original: string;
    droppedCount: number;
    droppedSize: number;
    dropped: string;
    state: string;
}

export interface DropRequestEntity {
    dropRequest: DropRequest;
}

export interface SubmitEmptyQueueRequest {
    connectionId: string;
}

export interface SubmitEmptyQueuesRequest {
    processGroupId: string;
}

export interface PollEmptyQueueSuccess {
    dropEntity: DropRequestEntity;
}

export interface ShowEmptyQueueResults {
    dropEntity: DropRequestEntity;
}

export interface QueueState {
    dropEntity: DropRequestEntity | null;
    connectionId: string | null;
    processGroupId: string | null;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
