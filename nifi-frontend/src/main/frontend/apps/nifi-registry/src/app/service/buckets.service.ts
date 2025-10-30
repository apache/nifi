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

import { Injectable, inject } from '@angular/core';
import { forkJoin, Observable, of, throwError } from 'rxjs';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Bucket } from '../state/buckets';
import { CreateBucketRequest, DeleteBucketRequest } from '../state/buckets/buckets.actions';
import { catchError } from 'rxjs/operators';

export type PolicyAction = 'read' | 'write' | 'delete';

export interface PolicySubject {
    identifier: string;
    identity: string;
    type: 'user' | 'group';
    [key: string]: unknown;
}

export interface PolicyRevision {
    version: number;
    clientId?: string;
}

export interface Policy {
    identifier: string;
    action: PolicyAction;
    resource: string;
    users: PolicySubject[];
    userGroups: PolicySubject[];
    revision: PolicyRevision;
    [key: string]: unknown;
}

export interface SaveBucketPolicyRequest {
    bucketId: string;
    action: PolicyAction;
    policyId?: string;
    users: PolicySubject[];
    userGroups: PolicySubject[];
    revision?: PolicyRevision;
}

export interface BucketPolicyTenants {
    users: PolicySubject[];
    userGroups: PolicySubject[];
}

@Injectable({ providedIn: 'root' })
export class BucketsService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-registry-api';

    getBuckets(): Observable<Bucket[]> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/buckets`,
        //     error: {
        //         message: 'Mock error: unable to GET buckets.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.get<Bucket[]>(`${BucketsService.API}/buckets`);
    }

    createBucket(request: CreateBucketRequest): Observable<Bucket> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/buckets`,
        //     error: {
        //         message: 'Mock error: unable to create bucket.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.post<Bucket>(`${BucketsService.API}/buckets`, {
            ...request,
            revision: {
                version: 0
            }
        });
    }

    updateBucket(request: { bucket: Bucket }): Observable<Bucket> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/buckets`,
        //     error: {
        //         message: 'Mock error: unable to update bucket.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.put<Bucket>(
            `${BucketsService.API}/buckets/${request.bucket.identifier}`,
            request.bucket
        );
    }

    deleteBucket(request: DeleteBucketRequest): Observable<Bucket> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/buckets`,
        //     error: {
        //         message: 'Mock error: unable to delete bucket.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.delete<Bucket>(
            `${BucketsService.API}/buckets/${request.bucket.identifier}?version=${request.version}`
        );
    }

    getPolicies(): Observable<Policy[]> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/policies`,
        //     error: {
        //         message: 'Mock error: unable to get policies.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.get<Policy[]>(`${BucketsService.API}/policies`);
    }

    getBucketPolicyTenants(): Observable<BucketPolicyTenants> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Bad Gateway',
        //     url: `${BucketsService.API}/tenants/users`,
        //     error: {
        //         message: 'Mock error: unable to get bucket policy tenents.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return forkJoin({
            users: this.httpClient
                .get<PolicySubject[]>(`${BucketsService.API}/tenants/users`)
                .pipe(catchError((error: HttpErrorResponse) => this.handleTenantError(error))),
            userGroups: this.httpClient
                .get<PolicySubject[]>(`${BucketsService.API}/tenants/user-groups`)
                .pipe(catchError((error: HttpErrorResponse) => this.handleTenantError(error)))
        });
    }

    saveBucketPolicy(request: SaveBucketPolicyRequest): Observable<Policy> {
        const resource = this.buildResourcePath(request.bucketId);
        const payload = this.buildPolicyPayload(request, resource);

        if (request.policyId) {
            return this.httpClient
                .put<Policy>(`${BucketsService.API}/policies/${request.policyId}`, payload)
                .pipe(catchError((error: HttpErrorResponse) => throwError(() => error)));
        }

        return this.httpClient
            .post<Policy>(`${BucketsService.API}/policies`, payload)
            .pipe(catchError((error: HttpErrorResponse) => throwError(() => error)));
    }

    private buildPolicyPayload(request: SaveBucketPolicyRequest, resource: string) {
        return {
            identifier: request.policyId ?? null,
            action: request.action,
            resource,
            users: request.users,
            userGroups: request.userGroups,
            revision: request.policyId ? (request.revision ?? { version: 0 }) : { version: 0 }
        };
    }

    private buildResourcePath(bucketId: string): string {
        return `/buckets/${bucketId}`;
    }

    private handleTenantError(error: HttpErrorResponse): Observable<PolicySubject[]> {
        if (error.status === 404) {
            return of([]);
        }
        return throwError(() => error);
    }
}
