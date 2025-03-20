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

import { Bucket } from '../../state/buckets';

export const dropletsFeatureKey = 'droplets';

export interface LoadDropletsResponse {
    droplets: Droplets[];
}

export interface Params {
    rel: string;
}

export interface Link {
    href: string;
    params: Params;
}

export interface Revision {
    version: number;
}

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
}

export interface Droplets {
    bucketIdentifier: string;
    bucketName: string;
    createdTimestamp: number;
    description: string;
    identifier: string;
    link: Link;
    modifiedTimestamp: number;
    name: string;
    permissions: Permissions;
    revision: Revision;
    type: string;
    versionCount: number;
}

export interface DropletsState {
    droplets: Droplets[];
    status: 'pending' | 'loading' | 'success';
    saving: boolean;
}

export interface DeleteDropletRequest {
    droplet: Droplets;
}

export interface DeleteDropletResponse {
    droplet: Droplets;
}

export interface ImportDropletDialog {
    buckets: Bucket[];
    activeBucket: Bucket | null;
}

export interface ImportDropletRequest {
    bucket: Bucket;
    file: File;
    name: string;
    description: string;
}
