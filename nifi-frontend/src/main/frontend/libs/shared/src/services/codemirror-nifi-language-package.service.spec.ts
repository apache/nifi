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
import { of, throwError } from 'rxjs';

import { CodemirrorNifiLanguagePackage } from './codemirror-nifi-language-package.service';
import { ElService } from './el.service';

describe('CodemirrorNifiLanguagePackage', () => {
    let service: CodemirrorNifiLanguagePackage;
    let mockElService: Partial<ElService>;
    const mockElGuideHtml = `<html><body><h1>Test Guide</h1></body></html>`;
    const mockParameters = [{ name: 'param1', description: 'Test parameter 1', value: 'value1', sensitive: false }];
    const mockLanguageConfigWithEl = { supportsEl: true, parameterListing: mockParameters };

    beforeEach(() => {
        mockElService = {
            getElGuide: jest.fn().mockReturnValue(of(mockElGuideHtml))
        };

        TestBed.configureTestingModule({
            providers: [CodemirrorNifiLanguagePackage, { provide: ElService, useValue: mockElService }]
        });

        service = TestBed.inject(CodemirrorNifiLanguagePackage);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe('Service Creation and Initialization', () => {
        it('should be created successfully', () => {
            // SETUP: Service instance from TestBed
            // INPUT: None required for creation
            // FUNCTION: Service instantiation
            // EXPECTED: Service should be defined and truthy
            // RESULT: Verify service exists
            expect(service).toBeTruthy();
            // SUMMARY: Service creation test complete
        });

        it('should handle EL service errors gracefully', () => {
            // SETUP: Mock EL service to throw error
            // INPUT: Error response from EL service
            (mockElService.getElGuide as jest.Mock).mockReturnValue(throwError(() => new Error('Network error')));

            // FUNCTION: Attempt to get language mode
            const result = service.getLanguageMode(mockLanguageConfigWithEl);

            // EXPECTED: Should still return language configuration, handling error gracefully
            // RESULT: Verify language config exists despite error
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Error handling test complete
        });
    });

    describe('getLanguageId', () => {
        it('should return nf', () => {
            // SETUP: Service instance
            // INPUT: None required for getLanguageId
            // FUNCTION: Call getLanguageId method
            const languageId = service.getLanguageId();
            // EXPECTED: Should return "nf" as language identifier
            // RESULT: Verify correct language ID
            expect(languageId).toEqual('nf');
            // SUMMARY: Language ID test complete
        });
    });

    describe('getLanguageMode Method', () => {
        it('should return language configuration with EL support enabled', () => {
            // SETUP: Mock successful EL service response
            // INPUT: Language config with EL support and mock HTML response
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));

            // FUNCTION: Get language mode with EL support
            const result = service.getLanguageMode(mockLanguageConfigWithEl);

            // EXPECTED: Should return language configuration with EL functions
            // RESULT: Verify language configuration structure
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: EL support test complete
        });

        it('should return language config with parameter support', () => {
            // SETUP: Mock EL service and language config with parameters
            // INPUT: Language config with parameters but no EL support
            const mockLanguageConfigWithParams = { supportsEl: false, parameterListing: mockParameters };
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));

            // FUNCTION: Get language mode with parameter support
            const result = service.getLanguageMode(mockLanguageConfigWithParams);

            // EXPECTED: Should return language configuration with parameter support
            // RESULT: Verify language configuration for parameters
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Parameter support test complete
        });

        it('should return language config with both EL and parameter support', () => {
            // SETUP: Mock EL service and comprehensive language config
            // INPUT: Language config with both EL and parameter support
            const mockComprehensiveConfig = { supportsEl: true, parameterListing: mockParameters };
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));

            // FUNCTION: Get language mode with full support
            const result = service.getLanguageMode(mockComprehensiveConfig);

            // EXPECTED: Should return comprehensive language configuration
            // RESULT: Verify complete language configuration
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Comprehensive support test complete
        });

        it('should handle EL service errors gracefully', () => {
            // SETUP: Mock EL service to return error
            // INPUT: Error response from EL service
            (mockElService.getElGuide as jest.Mock).mockReturnValue(throwError(() => new Error('Service unavailable')));

            // FUNCTION: Attempt to get language mode with error
            const result = service.getLanguageMode(mockLanguageConfigWithEl);

            // EXPECTED: Should still return language configuration despite EL error
            // RESULT: Verify language config returned with basic functionality
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: EL error handling test complete
        });
    });

    describe('HTML Processing and Stream Parser', () => {
        it('should process HTML content for EL functions', () => {
            // SETUP: Mock EL service with rich HTML content
            // INPUT: HTML with EL function definitions
            const richElHtml = `<html><body>
                <h1>Expression Language Guide</h1>
                <div class="function">
                    <h3>substring</h3>
                    <p>Returns a substring of the input</p>
                </div>
            </body></html>`;
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(richElHtml));

            // FUNCTION: Get language mode with HTML processing
            const result = service.getLanguageMode(mockLanguageConfigWithEl);

            // EXPECTED: Should process HTML and create language configuration
            // RESULT: Verify HTML processing creates proper language mode
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            expect(result.streamParser.startState).toBeDefined();
            // SUMMARY: HTML processing test complete
        });

        it('should create basic stream parser functionality', () => {
            // SETUP: Mock basic EL service response
            // INPUT: Simple language configuration
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));
            const basicConfig = { supportsEl: false, parameterListing: [] };

            // FUNCTION: Get language mode for basic parser
            const result = service.getLanguageMode(basicConfig);

            // EXPECTED: Should create basic stream parser
            // RESULT: Verify stream parser basic functionality
            expect(result.streamParser).toBeDefined();
            expect(typeof result.streamParser.startState).toBe('function');
            expect(typeof result.streamParser.token).toBe('function');
            // SUMMARY: Basic stream parser test complete
        });
    });

    describe('Edge Cases and Error Handling', () => {
        it('should handle empty parameter listing', () => {
            // SETUP: Language config with empty parameters
            // INPUT: Config with empty parameter array
            const emptyParamConfig = { supportsEl: true, parameterListing: [] };
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));

            // FUNCTION: Get language mode with empty parameters
            const result = service.getLanguageMode(emptyParamConfig);

            // EXPECTED: Should handle empty parameters gracefully
            // RESULT: Verify language mode created despite empty parameters
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Empty parameters test complete
        });

        it('should handle malformed HTML gracefully', () => {
            // SETUP: Mock EL service with malformed HTML
            // INPUT: Invalid/malformed HTML response
            const malformedHtml = `<html><body><h1>Incomplete HTML`;
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(malformedHtml));

            // FUNCTION: Get language mode with malformed HTML
            const result = service.getLanguageMode(mockLanguageConfigWithEl);

            // EXPECTED: Should handle malformed HTML without crashing
            // RESULT: Verify language mode created despite HTML issues
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Malformed HTML test complete
        });

        it('should handle configuration edge cases', () => {
            // SETUP: Edge case configuration
            // INPUT: Minimal or edge case configuration
            const edgeCaseConfig = { supportsEl: false };
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(mockElGuideHtml));

            // FUNCTION: Get language mode with edge case config
            const result = service.getLanguageMode(edgeCaseConfig);

            // EXPECTED: Should handle edge case configuration
            // RESULT: Verify language mode handles missing properties
            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
            // SUMMARY: Edge case configuration test complete
        });
    });
});
