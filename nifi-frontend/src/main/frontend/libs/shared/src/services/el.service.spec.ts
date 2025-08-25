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
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { HttpErrorResponse } from '@angular/common/http';

import { ElService } from './el.service';

describe('ElService', () => {
    let service: ElService;
    let httpMock: HttpTestingController;
    const expectedUrl = '../nifi-api/html/expression-language-guide.html';

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [HttpClientTestingModule],
            providers: [ElService]
        });

        service = TestBed.inject(ElService);
        httpMock = TestBed.inject(HttpTestingController);
    });

    afterEach(() => {
        httpMock.verify();
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    it('should fetch EL guide content', (done) => {
        const mockContent = '<div class="function"><h3>test</h3></div>';

        service.getElGuide().subscribe({
            next: (result) => {
                expect(result).toBe(mockContent);
                expect(result).toContain('<div class="function">');
                done();
            }
        });

        const req = httpMock.expectOne(expectedUrl);
        expect(req.request.method).toBe('GET');
        expect(req.request.responseType).toBe('text');
        req.flush(mockContent);
    });

    it('should handle 404 error', (done) => {
        service.getElGuide().subscribe({
            next: () => fail('Should have failed'),
            error: (error: HttpErrorResponse) => {
                expect(error.status).toBe(404);
                done();
            }
        });

        const req = httpMock.expectOne(expectedUrl);
        req.flush('Not Found', { status: 404, statusText: 'Not Found' });
    });

    it('should handle network error', (done) => {
        service.getElGuide().subscribe({
            next: () => fail('Should have failed'),
            error: (error) => {
                expect(error).toBeInstanceOf(HttpErrorResponse);
                expect(error.error).toBeInstanceOf(ProgressEvent);
                done();
            }
        });

        const req = httpMock.expectOne(expectedUrl);
        req.error(new ProgressEvent('error'));
    });

    it('should handle empty response', (done) => {
        service.getElGuide().subscribe({
            next: (result) => {
                expect(result).toBe('');
                done();
            }
        });

        const req = httpMock.expectOne(expectedUrl);
        req.flush('');
    });

    it('should be singleton service', () => {
        const service1 = TestBed.inject(ElService);
        const service2 = TestBed.inject(ElService);
        expect(service1).toBe(service2);
    });
});
