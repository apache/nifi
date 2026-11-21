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

import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { firstValueFrom } from 'rxjs';
import { UnsafeApiPathError } from '@nifi/shared';
import { ProcessGroupDrainService } from './process-group-drain.service';
import { createProcessGroupDrainRequestEntity } from '../testing/process-group-drain-fixture';

describe('ProcessGroupDrainService', () => {
    async function setup() {
        await TestBed.configureTestingModule({
            providers: [ProcessGroupDrainService, provideHttpClient(), provideHttpClientTesting()]
        }).compileComponents();

        return {
            service: TestBed.inject(ProcessGroupDrainService),
            http: TestBed.inject(HttpTestingController)
        };
    }

    afterEach(() => TestBed.inject(HttpTestingController).verify());

    it('POSTs an empty body to begin a drain', async () => {
        const { service, http } = await setup();
        const response = createProcessGroupDrainRequestEntity();
        const resultPromise = firstValueFrom(service.beginDrain('pg-1'));

        const request = http.expectOne('../nifi-api/process-groups/pg-1/drain-requests');
        expect(request.request.method).toBe('POST');
        expect(request.request.body).toEqual({});
        request.flush(response);

        expect(await resultPromise).toEqual(response);
    });

    it('GETs the drain request by process group and drain id', async () => {
        const { service, http } = await setup();
        const response = createProcessGroupDrainRequestEntity();
        const resultPromise = firstValueFrom(service.getDrain('pg-1', 'drain-1'));

        const request = http.expectOne('../nifi-api/process-groups/pg-1/drain-requests/drain-1');
        expect(request.request.method).toBe('GET');
        request.flush(response);

        expect(await resultPromise).toEqual(response);
    });

    it('DELETEs the drain request to cancel it', async () => {
        const { service, http } = await setup();
        const response = createProcessGroupDrainRequestEntity({ state: 'ENDED' });
        const resultPromise = firstValueFrom(service.endDrain('pg-1', 'drain-1'));

        const request = http.expectOne('../nifi-api/process-groups/pg-1/drain-requests/drain-1');
        expect(request.request.method).toBe('DELETE');
        request.flush(response);

        expect(await resultPromise).toEqual(response);
    });

    it.each([
        ['beginDrain', (service: ProcessGroupDrainService) => service.beginDrain('../bad')],
        ['getDrain', (service: ProcessGroupDrainService) => service.getDrain('../bad', 'drain-1')],
        ['endDrain', (service: ProcessGroupDrainService) => service.endDrain('pg-1', '../bad')]
    ])('rejects an unsafe id in %s before issuing a request', async (_name, invoke) => {
        const { service, http } = await setup();

        await expect(firstValueFrom(invoke(service))).rejects.toBeInstanceOf(UnsafeApiPathError);
        http.expectNone(() => true);
    });
});
