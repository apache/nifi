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

export interface Revision {
    version: number;
    clientId?: string;
    lastModifier?: string;
}

export interface RevisionRequest {
    version?: number;
    clientId?: string;
}

export interface CreateRevisionRequest extends RevisionRequest {
    version: 0;
}

export interface Position {
    x: number;
    y: number;
}

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
}

export interface Bundle {
    artifact: string;
    group: string;
    version: string;
}

export interface BulletinEntity {
    canRead: boolean;
    id: number;
    sourceId: string;
    groupId: string;
    timestamp: string;
    timestampIso: string;
    nodeAddress?: string;
    bulletin?: {
        id: number;
        sourceId: string;
        groupId: string;
        category: string;
        level: string;
        message: string;
        stackTrace?: string;
        sourceName: string;
        timestamp: string;
        timestampIso: string;
        nodeAddress?: string;
        sourceType: string;
    };
}

export type ReadableBulletinEntity = BulletinEntity & Required<Pick<BulletinEntity, 'bulletin'>>;
