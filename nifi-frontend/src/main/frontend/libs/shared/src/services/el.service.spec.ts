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
import { HttpClient, HttpErrorResponse } from '@angular/common/http';

import { ElService } from './el.service';

/**
 * Test Suite for ElService using SIFERS approach:
 * S - Setup: Test environment configuration
 * I - Input: Test data and parameters
 * F - Function: Method being tested
 * E - Expected: Expected outcomes
 * R - Result: Actual test results
 * S - Summary: Cleanup and verification
 */
describe('ElService', () => {
    // SETUP: Test environment variables
    let service: ElService;
    let httpMock: HttpTestingController;

    // SETUP: Mock data
    const mockElGuideHtml = `<html>
<body>
<div class="function">
<h3>substring</h3>
<span class="description">Returns a portion of the string</span>
<span class="returnType">String</span>
<span class="subject">String</span>
<span class="argName">start</span>
<span class="argDesc">Starting index</span>
</div>
</body>
</html>`;

    const expectedUrl = '../nifi-api/html/expression-language-guide.html';

    // SETUP: Configure test environment before each test
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [HttpClientTestingModule],
            providers: [ElService]
        });

        service = TestBed.inject(ElService);
        httpMock = TestBed.inject(HttpTestingController);
    });

    // SUMMARY: Verify no outstanding HTTP requests after each test
    afterEach(() => {
        httpMock.verify();
    });

    describe('Service Creation', () => {
        it('should be created successfully', () => {
            // SETUP: Service is injected in beforeEach
            // INPUT: No input needed for constructor test
            // FUNCTION: Service instantiation
            // EXPECTED: Service should be truthy and have correct dependencies
            // RESULT: Verify service instance
            expect(service).toBeTruthy();
            expect(service).toBeInstanceOf(ElService);
            // SUMMARY: Service creation test complete
        });
    });

    describe('getElGuide Method', () => {
        it('should successfully fetch EL guide HTML content', (done) => {
            // SETUP: Service and HTTP mock configured
            // INPUT: Mock HTML response data
            const inputData = mockElGuideHtml;

            // FUNCTION: Call getElGuide method
            service.getElGuide().subscribe({
                next: (result) => {
                    // EXPECTED: Should receive the mock HTML content
                    // RESULT: Verify the actual result matches expected
                    expect(result).toBe(inputData);
                    expect(typeof result).toBe('string');
                    expect(result).toContain('<div class="function">');
                    expect(result).toContain('substring');
                    done();
                },
                error: (error) => {
                    fail('Should not have failed: ' + error);
                    done();
                }
            });

            // EXPECTED: HTTP GET request to correct URL with text response type
            const req = httpMock.expectOne(expectedUrl);
            expect(req.request.method).toBe('GET');
            expect(req.request.responseType).toBe('text');

            // RESULT: Provide mock response
            req.flush(inputData);

            // SUMMARY: HTTP request verified and response delivered
        });

        it('should handle HTTP error responses gracefully', (done) => {
            // SETUP: Service and HTTP mock configured
            // INPUT: Error response configuration
            const errorMessage = 'Failed to load EL guide';
            const errorStatus = 404;
            const errorStatusText = 'Not Found';

            // FUNCTION: Call getElGuide method expecting error
            service.getElGuide().subscribe({
                next: () => {
                    fail('Should have failed with error');
                    done();
                },
                error: (error: HttpErrorResponse) => {
                    // EXPECTED: Should receive HTTP error response
                    // RESULT: Verify error details
                    expect(error).toBeInstanceOf(HttpErrorResponse);
                    expect(error.status).toBe(errorStatus);
                    expect(error.statusText).toBe(errorStatusText);
                    expect(error.error).toBe(errorMessage);
                    done();
                }
            });

            // EXPECTED: HTTP GET request to correct URL
            const req = httpMock.expectOne(expectedUrl);
            expect(req.request.method).toBe('GET');

            // RESULT: Simulate error response
            req.flush(errorMessage, { status: errorStatus, statusText: errorStatusText });

            // SUMMARY: Error handling verified
        });

        it('should handle network timeout errors', (done) => {
            // SETUP: Service and HTTP mock configured
            // INPUT: Timeout error configuration
            const timeoutError = new ProgressEvent('timeout');

            // FUNCTION: Call getElGuide method expecting timeout
            service.getElGuide().subscribe({
                next: () => {
                    fail('Should have failed with timeout');
                    done();
                },
                error: (error) => {
                    // EXPECTED: Should receive timeout error
                    // RESULT: Verify error is timeout type
                    expect(error).toBeInstanceOf(HttpErrorResponse);
                    expect(error.error).toBeInstanceOf(ProgressEvent);
                    expect(error.error.type).toBe('timeout');
                    done();
                }
            });

            // EXPECTED: HTTP GET request to correct URL
            const req = httpMock.expectOne(expectedUrl);

            // RESULT: Simulate timeout error
            req.error(timeoutError);

            // SUMMARY: Timeout error handling verified
        });

        it('should use correct API endpoint URL', () => {
            // SETUP: Service configured
            // INPUT: No input data needed
            // FUNCTION: Trigger HTTP request to verify URL
            service.getElGuide().subscribe();

            // EXPECTED: Request to specific API endpoint
            const req = httpMock.expectOne(expectedUrl);

            // RESULT: Verify URL matches expected pattern
            expect(req.request.url).toBe('../nifi-api/html/expression-language-guide.html');
            expect(req.request.url).toContain('nifi-api');
            expect(req.request.url).toContain('expression-language-guide.html');

            req.flush('test response');

            // SUMMARY: API endpoint verification complete
        });

        it('should request response as text type', () => {
            // SETUP: Service configured
            // INPUT: No input data needed
            // FUNCTION: Trigger HTTP request to verify response type
            service.getElGuide().subscribe();

            // EXPECTED: Request with responseType: 'text'
            const req = httpMock.expectOne(expectedUrl);

            // RESULT: Verify response type configuration
            expect(req.request.responseType).toBe('text');
            expect(req.request.responseType).not.toBe('json');

            req.flush('test response');

            // SUMMARY: Response type verification complete
        });

        it('should handle empty response gracefully', (done) => {
            // SETUP: Service and HTTP mock configured
            // INPUT: Empty string response
            const emptyResponse = '';

            // FUNCTION: Call getElGuide with empty response
            service.getElGuide().subscribe({
                next: (result) => {
                    // EXPECTED: Should handle empty string without error
                    // RESULT: Verify empty response is handled
                    expect(result).toBe('');
                    expect(typeof result).toBe('string');
                    done();
                },
                error: (error) => {
                    fail('Should not fail on empty response: ' + error);
                    done();
                }
            });

            // EXPECTED: HTTP request as usual
            const req = httpMock.expectOne(expectedUrl);

            // RESULT: Return empty response
            req.flush(emptyResponse);

            // SUMMARY: Empty response handling verified
        });

        it('should handle malformed HTML gracefully', (done) => {
            // SETUP: Service and HTTP mock configured
            // INPUT: Malformed HTML content
            const malformedHtml = '<html><body><div class="function"><h3>test</h3><span class="description">test';

            // FUNCTION: Call getElGuide with malformed HTML
            service.getElGuide().subscribe({
                next: (result) => {
                    // EXPECTED: Should return the malformed HTML as-is (service doesn't parse)
                    // RESULT: Verify malformed HTML is returned without processing
                    expect(result).toBe(malformedHtml);
                    expect(result).toContain('<div class="function">');
                    expect(result).not.toContain('</body></html>'); // Incomplete HTML
                    done();
                },
                error: (error) => {
                    fail('Should not fail on malformed HTML: ' + error);
                    done();
                }
            });

            // EXPECTED: HTTP request processes normally
            const req = httpMock.expectOne(expectedUrl);

            // RESULT: Return malformed HTML
            req.flush(malformedHtml);

            // SUMMARY: Malformed HTML handling verified
        });
    });

    describe('Service Configuration', () => {
        it('should have correct DOCS constant value', () => {
            // SETUP: Service instantiated
            // INPUT: No input needed for constant test
            // FUNCTION: Access private static constant via behavior
            // EXPECTED: DOCS should be '../nifi-api'

            // Trigger a request to verify the URL construction uses correct DOCS value
            service.getElGuide().subscribe();
            const req = httpMock.expectOne((request) => request.url.startsWith('../nifi-api'));

            // RESULT: Verify URL starts with expected DOCS path
            expect(req.request.url).toContain('../nifi-api');
            req.flush('test');

            // SUMMARY: DOCS constant verification complete
        });

        it('should be provided as singleton (root service)', () => {
            // SETUP: Create second service instance
            // INPUT: No input needed
            // FUNCTION: Inject service twice
            const service1 = TestBed.inject(ElService);
            const service2 = TestBed.inject(ElService);

            // EXPECTED: Both instances should be the same (singleton)
            // RESULT: Verify singleton behavior
            expect(service1).toBe(service2);
            expect(service1).toBe(service);

            // SUMMARY: Singleton service verification complete
        });
    });

    describe('Error Scenarios', () => {
        it('should handle server errors (5xx)', (done) => {
            // SETUP: Service configured for server error test
            // INPUT: Server error configuration
            const serverError = 'Internal Server Error';
            const errorStatus = 500;

            // FUNCTION: Call service method expecting server error
            service.getElGuide().subscribe({
                next: () => {
                    fail('Should have failed with server error');
                    done();
                },
                error: (error: HttpErrorResponse) => {
                    // EXPECTED: Should handle server error appropriately
                    // RESULT: Verify server error handling
                    expect(error.status).toBe(errorStatus);
                    expect(error.error).toBe(serverError);
                    done();
                }
            });

            const req = httpMock.expectOne(expectedUrl);
            req.flush(serverError, { status: errorStatus, statusText: 'Internal Server Error' });

            // SUMMARY: Server error handling verified
        });

        it('should handle client errors (4xx)', (done) => {
            // SETUP: Service configured for client error test
            // INPUT: Client error configuration
            const clientError = 'Bad Request';
            const errorStatus = 400;

            // FUNCTION: Call service method expecting client error
            service.getElGuide().subscribe({
                next: () => {
                    fail('Should have failed with client error');
                    done();
                },
                error: (error: HttpErrorResponse) => {
                    // EXPECTED: Should handle client error appropriately
                    // RESULT: Verify client error handling
                    expect(error.status).toBe(errorStatus);
                    expect(error.error).toBe(clientError);
                    done();
                }
            });

            const req = httpMock.expectOne(expectedUrl);
            req.flush(clientError, { status: errorStatus, statusText: 'Bad Request' });

            // SUMMARY: Client error handling verified
        });
    });
});
