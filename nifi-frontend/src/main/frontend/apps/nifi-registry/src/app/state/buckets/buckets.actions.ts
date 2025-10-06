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

import { createAction, props } from '@ngrx/store';
import { Bucket, LoadBucketsResponse } from '.';

export interface CreateBucketRequest {
    name: string;
    description: string;
    allowPublicRead: boolean;
}

export interface DeleteBucketRequest {
    bucket: Bucket;
    version: number;
}

export const loadBuckets = createAction('[Buckets] Load Buckets');

export const loadBucketsSuccess = createAction(
    '[Buckets] Load Buckets Success',
    props<{ response: LoadBucketsResponse }>()
);

export const openCreateBucketDialog = createAction('[Buckets] Open Create Bucket Dialog');

export const createBucket = createAction(
    '[Buckets] Create Bucket',
    props<{ request: CreateBucketRequest; keepDialogOpen: boolean }>()
);

export const createBucketSuccess = createAction(
    '[Buckets] Create Bucket Success',
    props<{ response: Bucket; keepDialogOpen: boolean }>()
);

export const createBucketFailure = createAction('[Buckets] Create Bucket Failure');

export const openEditBucketDialog = createAction(
    '[Buckets] Open Edit Bucket Dialog',
    props<{ request: { bucket: Bucket } }>()
);

export const updateBucket = createAction('[Buckets] Update Bucket', props<{ request: { bucket: Bucket } }>());

export const updateBucketSuccess = createAction('[Buckets] Update Bucket Success', props<{ response: Bucket }>());

export const updateBucketFailure = createAction('[Buckets] Update Bucket Failure');

export const openDeleteBucketDialog = createAction(
    '[Buckets] Open Delete Bucket Dialog',
    props<{ request: { bucket: Bucket } }>()
);

export const deleteBucket = createAction('[Buckets] Delete Bucket', props<{ request: DeleteBucketRequest }>());

export const deleteBucketSuccess = createAction('[Buckets] Delete Bucket Success', props<{ response: Bucket }>());

export const deleteBucketFailure = createAction('[Buckets] Delete Bucket Failure');

export const openManageBucketPoliciesDialog = createAction(
    '[Buckets] Open Manage Bucket Policies Dialog',
    props<{ request: { bucket: Bucket } }>()
);
