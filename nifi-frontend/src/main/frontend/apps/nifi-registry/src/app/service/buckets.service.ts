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
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Bucket } from '../state/buckets';
import { CreateBucketRequest, DeleteBucketRequest } from '../state/buckets/buckets.actions';

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
}
