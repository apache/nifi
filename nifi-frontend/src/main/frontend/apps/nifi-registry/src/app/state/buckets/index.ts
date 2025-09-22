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

import { Permissions } from '@nifi/shared';

export const bucketsFeatureKey = 'buckets';

export interface LoadBucketsResponse {
    buckets: Bucket[];
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

export interface Bucket {
    allowBundleRedeploy: boolean;
    allowPublicRead: boolean;
    createdTimestamp: number;
    description: string;
    identifier: string;
    link: Link;
    name: string;
    permissions: Permissions;
    revision: Revision;
}

export interface BucketsState {
    buckets: Bucket[];
    status: 'pending' | 'loading' | 'success';
}
