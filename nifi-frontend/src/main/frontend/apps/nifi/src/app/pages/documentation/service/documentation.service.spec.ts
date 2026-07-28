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

import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { DocumentationService } from './documentation.service';
import { DefinitionCoordinates } from '../state';

describe('DocumentationService', () => {
    function createCoordinates(overrides: Partial<DefinitionCoordinates> = {}): DefinitionCoordinates {
        return {
            group: 'org.apache.nifi',
            artifact: 'nifi-standard-nar',
            version: '1.0.0',
            type: 'org.apache.nifi.processors.standard.LogAttribute',
            ...overrides
        };
    }

    async function setup() {
        return {
            service: TestBed.inject(DocumentationService),
            httpMock: TestBed.inject(HttpTestingController)
        };
    }

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            providers: [DocumentationService, provideHttpClient(), provideHttpClientTesting()]
        }).compileComponents();
    });

    afterEach(() => {
        TestBed.inject(HttpTestingController).verify();
    });

    describe('getProcessorDefinition', () => {
        it('should GET the processor-definition endpoint for the coordinates', async () => {
            const { service, httpMock } = await setup();

            service.getProcessorDefinition(createCoordinates()).subscribe();

            httpMock
                .expectOne(
                    (r) =>
                        r.method === 'GET' &&
                        r.url ===
                            '../nifi-api/flow/processor-definition/org.apache.nifi/nifi-standard-nar/1.0.0/org.apache.nifi.processors.standard.LogAttribute'
                )
                .flush({});
        });

        it('should reject a path-traversal coordinate without issuing a request', async () => {
            const { service } = await setup();

            let error: unknown;
            service
                .getProcessorDefinition(createCoordinates({ type: '..%2F..%2Fnifi-api%2Fcontroller%2Fconfig' }))
                .subscribe({ error: (e) => (error = e) });

            expect(error).toBeInstanceOf(Error);
        });
    });

    describe('getStepDocumentation', () => {
        it('should encode a step name containing reserved characters', async () => {
            const { service, httpMock } = await setup();

            service.getStepDocumentation(createCoordinates(), 'Step One?').subscribe();

            httpMock
                .expectOne(
                    (r) =>
                        r.method === 'GET' &&
                        r.url ===
                            '../nifi-api/flow/steps/org.apache.nifi/nifi-standard-nar/1.0.0/org.apache.nifi.processors.standard.LogAttribute/Step%20One%3F'
                )
                .flush({ stepDocumentation: '## Docs' });
        });

        it('should reject a path-traversal step name without issuing a request', async () => {
            const { service } = await setup();

            let error: unknown;
            service.getStepDocumentation(createCoordinates(), '../secrets').subscribe({ error: (e) => (error = e) });

            expect(error).toBeInstanceOf(Error);
        });
    });
});
