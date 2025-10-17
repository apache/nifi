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
import { ErrorHelper } from './error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../state/error/error.actions';

describe('ErrorHelper', () => {
    let service: ErrorHelper;

    beforeEach(() => {
        TestBed.configureTestingModule({});
        service = TestBed.inject(ErrorHelper);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('handleLoadingError', () => {
        describe('when hasExistingData is false (initial load)', () => {
            it('should return fullScreenError for any error status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 500,
                    error: 'Server error'
                });

                const result = service.handleLoadingError(false, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });

            it('should return fullScreenError for 403 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 403,
                    error: 'Forbidden'
                });

                const result = service.handleLoadingError(false, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });

            it('should return fullScreenError for 401 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 401,
                    error: 'Unauthorized'
                });

                const result = service.handleLoadingError(false, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });
        });

        describe('when hasExistingData is true (refresh/reload)', () => {
            it('should return snackBarError for 400 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 400,
                    error: 'Bad request'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
                expect((result as any).error).toBe('Bad request');
            });

            it('should return snackBarError for 403 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 403,
                    error: 'Forbidden'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
            });

            it('should return snackBarError for 404 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 404,
                    error: 'Not found'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
            });

            it('should return snackBarError for 409 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 409,
                    error: 'Conflict'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
            });

            it('should return snackBarError for 413 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 413,
                    error: 'Payload too large'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
            });

            it('should return snackBarError for 503 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 503,
                    error: 'Service unavailable'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.snackBarError.type);
            });

            it('should return fullScreenError for 401 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 401,
                    error: 'Unauthorized'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });

            it('should return fullScreenError for 500 status', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 500,
                    error: 'Internal server error'
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });

            it('should return fullScreenError for 0 status (network error)', () => {
                const errorResponse = new HttpErrorResponse({
                    status: 0,
                    error: null
                });

                const result = service.handleLoadingError(true, errorResponse);

                expect(result.type).toBe(ErrorActions.fullScreenError.type);
            });
        });
    });

    describe('showErrorInContext', () => {
        it('should return true for status codes that should show in context', () => {
            expect(service.showErrorInContext(400)).toBe(true);
            expect(service.showErrorInContext(403)).toBe(true);
            expect(service.showErrorInContext(404)).toBe(true);
            expect(service.showErrorInContext(409)).toBe(true);
            expect(service.showErrorInContext(413)).toBe(true);
            expect(service.showErrorInContext(503)).toBe(true);
        });

        it('should return false for status codes that should not show in context', () => {
            expect(service.showErrorInContext(401)).toBe(false);
            expect(service.showErrorInContext(500)).toBe(false);
            expect(service.showErrorInContext(502)).toBe(false);
            expect(service.showErrorInContext(0)).toBe(false);
        });
    });

    describe('getErrorString', () => {
        it('should return the error message from the response', () => {
            const errorResponse = new HttpErrorResponse({
                status: 500,
                error: 'Server error message'
            });

            const result = service.getErrorString(errorResponse);

            expect(result).toBe('Server error message');
        });

        it('should return error with prefix when provided', () => {
            const errorResponse = new HttpErrorResponse({
                status: 500,
                error: 'Server error message'
            });

            const result = service.getErrorString(errorResponse, 'Failed to load');

            expect(result).toBe('Failed to load - [Server error message]');
        });

        it('should handle status 0 (network error)', () => {
            const errorResponse = new HttpErrorResponse({
                status: 0,
                error: null
            });

            const result = service.getErrorString(errorResponse);

            expect(result).toBe('An unspecified error occurred.');
        });
    });
});
