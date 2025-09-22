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
import { Link, Revision, Permissions } from '../index';

export const dropletsFeatureKey = 'droplets';

export interface LoadDropletsResponse {
    droplets: Droplet[];
}

export interface Droplet {
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
    droplets: Droplet[];
    status: 'pending' | 'loading' | 'success';
    saving: boolean;
}

export interface DeleteDropletRequest {
    droplet: Droplet;
}

export interface ImportDropletDialog {
    buckets: Bucket[];
}

export interface ImportDropletRequest {
    bucket: Bucket;
    description: string;
    file: File;
    name: string;
}

export interface ImportDropletVersionRequest {
    href: string;
    file: File;
    description: string;
}
