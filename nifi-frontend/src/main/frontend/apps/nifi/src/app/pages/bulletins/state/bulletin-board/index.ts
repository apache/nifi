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

import { BulletinEntity } from '@nifi/shared';

export const bulletinBoardFeatureKey = 'bulletin-board';

export interface BulletinBoardEntity {
    bulletins: BulletinEntity[];
    generated: string;
}

export interface BulletinBoardEvent {
    type: 'auto-refresh' | 'filter';
    message: string;
}

export interface BulletinBoardItem {
    item: BulletinEntity | BulletinBoardEvent;
}

export interface BulletinBoardState {
    bulletinBoardItems: BulletinBoardItem[];
    filter: BulletinBoardFilterArgs;
    autoRefresh: boolean;
    lastBulletinId: number;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}

export interface LoadBulletinBoardResponse {
    bulletinBoard: BulletinBoardEntity;
    loadedTimestamp: string;
}

export interface LoadBulletinBoardRequest {
    after?: number;
    limit?: number;
    groupId?: string;
    sourceId?: string;
    sourceName?: string;
    message?: string;
}

export interface BulletinBoardFilterArgs {
    filterTerm: string;
    filterColumn: string;
}
