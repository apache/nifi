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
import { Router } from '@angular/router';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { of, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';

import { rootGroupGuard } from './root-group.guard';
import { FlowService } from '../../../service/flow.service';
import { ErrorHelper } from '../../../../../service/error-helper.service';
import { selectCurrentProcessGroupId } from '../../../state/flow/flow.selectors';
import { fullScreenError } from '../../../../../state/error/error.actions';

interface SetupOptions {
    currentProcessGroupId?: string;
    statusResponse?: any;
    statusError?: HttpErrorResponse;
}

function createMockStatusResponse(id = 'abc-123') {
    return { processGroupStatus: { id } };
}

async function setup(options: SetupOptions = {}) {
    const { currentProcessGroupId = 'root', statusResponse, statusError } = options;

    const mockFlowService = {
        getProcessGroupStatus: vi.fn()
    };

    if (statusError) {
        mockFlowService.getProcessGroupStatus.mockReturnValue(throwError(() => statusError));
    } else if (statusResponse) {
        mockFlowService.getProcessGroupStatus.mockReturnValue(of(statusResponse));
    }

    const mockRouter = {
        navigate: vi.fn().mockReturnValue(Promise.resolve(true))
    };

    const mockErrorHelper = {
        fullScreenError: vi.fn().mockImplementation((error: HttpErrorResponse) =>
            fullScreenError({
                errorDetail: {
                    title: 'Insufficient Permissions',
                    message: error.message
                }
            })
        )
    };

    await TestBed.configureTestingModule({
        providers: [
            provideMockStore(),
            { provide: FlowService, useValue: mockFlowService },
            { provide: Router, useValue: mockRouter },
            { provide: ErrorHelper, useValue: mockErrorHelper }
        ]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    store.overrideSelector(selectCurrentProcessGroupId, currentProcessGroupId);
    const dispatchSpy = vi.spyOn(store, 'dispatch');

    return { store, mockFlowService, mockRouter, mockErrorHelper, dispatchSpy };
}

describe('rootGroupGuard', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('when process group id is initial state (root)', () => {
        it('should navigate to the root process group on successful status response', async () => {
            const statusResponse = createMockStatusResponse('real-pg-id');
            const { mockRouter, mockFlowService } = await setup({
                currentProcessGroupId: 'root',
                statusResponse
            });

            const result = await new Promise((resolve) => {
                const guard$ = TestBed.runInInjectionContext(() => rootGroupGuard({} as any, {} as any));
                (guard$ as any).subscribe((val: any) => resolve(val));
            });

            expect(mockFlowService.getProcessGroupStatus).toHaveBeenCalled();
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/process-groups', 'real-pg-id']);
            expect(result).toBe(true);
        });

        it('should dispatch fullScreenError and return false on HTTP error', async () => {
            const statusError = new HttpErrorResponse({
                status: 403,
                statusText: 'Forbidden',
                error: { message: 'Access is denied' },
                url: '/nifi-api/flow/process-groups/root/status'
            });
            const { mockErrorHelper, dispatchSpy, mockRouter } = await setup({
                currentProcessGroupId: 'root',
                statusError
            });

            const result = await new Promise((resolve) => {
                const guard$ = TestBed.runInInjectionContext(() => rootGroupGuard({} as any, {} as any));
                (guard$ as any).subscribe((val: any) => resolve(val));
            });

            expect(result).toBe(false);
            expect(mockErrorHelper.fullScreenError).toHaveBeenCalledWith(statusError);
            expect(dispatchSpy).toHaveBeenCalled();
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });
    });

    describe('when process group id is already set', () => {
        it('should navigate directly to the existing process group', async () => {
            const existingPgId = 'existing-pg-id';
            const { mockRouter, mockFlowService } = await setup({
                currentProcessGroupId: existingPgId
            });

            const result = await new Promise((resolve) => {
                const guard$ = TestBed.runInInjectionContext(() => rootGroupGuard({} as any, {} as any));
                (guard$ as any).subscribe((val: any) => resolve(val));
            });

            expect(mockFlowService.getProcessGroupStatus).not.toHaveBeenCalled();
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/process-groups', existingPgId]);
            expect(result).toBe(true);
        });
    });
});
