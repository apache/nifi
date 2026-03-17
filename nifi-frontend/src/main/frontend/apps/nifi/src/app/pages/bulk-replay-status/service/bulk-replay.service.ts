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
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { BulkReplayJobDetail, BulkReplayJobItem, BulkReplayJobSummary } from '../state';

@Injectable({ providedIn: 'root' })
export class BulkReplayService {
    private httpClient = inject(HttpClient);

    private static readonly API = '../nifi-api/bulk-replay/jobs';

    getJobs(): Observable<BulkReplayJobSummary[]> {
        return this.httpClient
            .get<{ jobs: BulkReplayJobSummary[] }>(BulkReplayService.API)
            .pipe(map((r) => r.jobs ?? []));
    }

    getJobSummary(jobId: string): Observable<BulkReplayJobSummary> {
        return this.httpClient
            .get<{ job: BulkReplayJobSummary }>(`${BulkReplayService.API}/${encodeURIComponent(jobId)}`)
            .pipe(map((r) => r.job));
    }

    getJobItems(jobId: string): Observable<BulkReplayJobItem[]> {
        return this.httpClient
            .get<{ items: BulkReplayJobItem[] }>(`${BulkReplayService.API}/${encodeURIComponent(jobId)}/items`)
            .pipe(map((r) => r.items ?? []));
    }

    submitJob(detail: BulkReplayJobDetail): Observable<BulkReplayJobSummary> {
        return this.httpClient
            .post<{ job: BulkReplayJobSummary }>(BulkReplayService.API, { jobDetail: detail })
            .pipe(map((r) => r.job));
    }

    cancelJob(jobId: string): Observable<void> {
        return this.httpClient.put<void>(`${BulkReplayService.API}/${encodeURIComponent(jobId)}/cancel`, null);
    }

    deleteJob(jobId: string): Observable<void> {
        return this.httpClient.delete<void>(`${BulkReplayService.API}/${encodeURIComponent(jobId)}`);
    }

    getConfig(): Observable<{ nodeDisconnectTimeout: string }> {
        return this.httpClient.get<{ nodeDisconnectTimeout: string }>(`${BulkReplayService.API}/config`);
    }

    updateConfig(config: { nodeDisconnectTimeout: string }): Observable<{ nodeDisconnectTimeout: string }> {
        return this.httpClient.put<{ nodeDisconnectTimeout: string }>(`${BulkReplayService.API}/config`, config);
    }
}
