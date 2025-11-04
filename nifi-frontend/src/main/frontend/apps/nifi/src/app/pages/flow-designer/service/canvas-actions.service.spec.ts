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
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import * as d3 from 'd3';

import { CanvasActionsService } from './canvas-actions.service';
import { CanvasUtils } from './canvas-utils.service';
import { CanvasView } from './canvas-view.service';
import { CopyPasteService } from './copy-paste.service';
import { Client } from '../../../service/client.service';
import { CanvasState } from '../state';
import { flowFeatureKey } from '../state/flow';
import * as fromFlow from '../state/flow/flow.reducer';
import { transformFeatureKey } from '../state/transform';
import * as fromTransform from '../state/transform/transform.reducer';
import { controllerServicesFeatureKey } from '../state/controller-services';
import * as fromControllerServices from '../state/controller-services/controller-services.reducer';
import { parameterFeatureKey } from '../state/parameter';
import * as fromParameter from '../state/parameter/parameter.reducer';
import { queueFeatureKey } from '../../queue/state';
import * as fromQueue from '../state/queue/queue.reducer';
import { flowAnalysisFeatureKey } from '../state/flow-analysis';
import * as fromFlowAnalysis from '../state/flow-analysis/flow-analysis.reducer';
import * as FlowActions from '../state/flow/flow.actions';
import { ComponentType, NiFiCommon } from '@nifi/shared';

// Mock d3 module
jest.mock('d3', () => ({
    selectAll: jest.fn()
}));

describe('CanvasActionsService', () => {
    let service: CanvasActionsService;
    let store: MockStore;
    let mockNiFiCommon: jest.Mocked<NiFiCommon>;

    const initialState: CanvasState = {
        [flowFeatureKey]: fromFlow.initialState,
        [transformFeatureKey]: fromTransform.initialState,
        [controllerServicesFeatureKey]: fromControllerServices.initialState,
        [parameterFeatureKey]: fromParameter.initialState,
        [queueFeatureKey]: fromQueue.initialState,
        [flowAnalysisFeatureKey]: fromFlowAnalysis.initialState
    };

    beforeEach(() => {
        // Create mock NiFiCommon - only mock the methods we actually use in tests
        mockNiFiCommon = {
            getMostRecentBulletinTimestamp: jest.fn()
        } as any;

        TestBed.configureTestingModule({
            providers: [
                CanvasActionsService,
                provideMockStore({ initialState }),
                {
                    provide: NiFiCommon,
                    useValue: mockNiFiCommon
                },
                {
                    provide: Router,
                    useValue: {
                        navigate: jest.fn()
                    }
                },
                {
                    provide: CanvasUtils,
                    useValue: {
                        isProcessor: jest.fn().mockReturnValue(false),
                        isRemoteProcessGroup: jest.fn().mockReturnValue(false),
                        isLabel: jest.fn().mockReturnValue(false),
                        isConnection: jest.fn().mockReturnValue(false),
                        canRead: jest.fn().mockReturnValue(true),
                        canWrite: jest.fn().mockReturnValue(true),
                        canCopy: jest.fn().mockReturnValue(true),
                        canModify: jest.fn().mockReturnValue(true),
                        canDelete: jest.fn().mockReturnValue(true),
                        getComponentByType: jest.fn(),
                        moveToFront: jest.fn(),
                        moveToBack: jest.fn(),
                        getSelectionByComponentType: jest.fn()
                    }
                },
                {
                    provide: CanvasView,
                    useValue: {
                        updateCanvasVisibility: jest.fn(),
                        centerBoundingBox: jest.fn(),
                        isCanvasInitialized: jest.fn().mockReturnValue(false)
                    }
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: jest.fn()
                    }
                },
                {
                    provide: Client,
                    useValue: {
                        isSecure: jest.fn().mockReturnValue(false)
                    }
                },
                {
                    provide: CopyPasteService,
                    useValue: {
                        isCopiedContentInView: jest.fn(),
                        toOffsetPasteRequest: jest.fn(),
                        toCenteredPasteRequest: jest.fn(),
                        paste: jest.fn()
                    }
                }
            ]
        });
        service = TestBed.inject(CanvasActionsService);
        store = TestBed.inject(MockStore);

        jest.spyOn(store, 'dispatch');
    });

    describe('clearBulletins action', () => {
        let mockProcessGroupId: string;
        let clearBulletinsAction: any;

        beforeEach(() => {
            mockProcessGroupId = 'test-process-group-id';
            jest.spyOn(service, 'currentProcessGroupId').mockReturnValue(mockProcessGroupId);
            clearBulletinsAction = service.getAction('clearBulletins');
        });

        describe('condition', () => {
            let clearBulletinsCondition: any;
            let mockD3SelectAll: jest.MockedFunction<typeof d3.selectAll>;

            beforeEach(() => {
                clearBulletinsCondition = service.getConditionFunction('clearBulletins');
                mockD3SelectAll = d3.selectAll as jest.MockedFunction<typeof d3.selectAll>;
            });

            afterEach(() => {
                jest.clearAllMocks();
            });

            it('should return true when selection is empty and components with bulletins exist', () => {
                // Create a mock empty selection
                const mockEmptySelection = {
                    empty: jest.fn().mockReturnValue(true)
                };

                // Mock d3.selectAll to return non-empty selection for components with bulletins
                mockD3SelectAll.mockReturnValue({
                    empty: jest.fn().mockReturnValue(false)
                } as any);

                const result = clearBulletinsCondition(mockEmptySelection);

                expect(result).toBe(true);
                expect(mockD3SelectAll).toHaveBeenCalledWith('g.component.has-bulletins');
            });

            it('should return false when selection is empty and no components with bulletins exist', () => {
                const mockEmptySelection = {
                    empty: jest.fn().mockReturnValue(true)
                };

                // Mock d3.selectAll to return empty selection (no components with bulletins)
                mockD3SelectAll.mockReturnValue({
                    empty: jest.fn().mockReturnValue(true)
                } as any);

                const result = clearBulletinsCondition(mockEmptySelection);

                expect(result).toBe(false);
            });

            it('should return true for single component with bulletins and write permissions', () => {
                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue({
                        type: ComponentType.Processor,
                        permissions: { canWrite: true }
                    }),
                    classed: jest.fn().mockReturnValue(true)
                };

                const result = clearBulletinsCondition(mockSelection);

                expect(result).toBe(true);
                expect(mockSelection.classed).toHaveBeenCalledWith('has-bulletins');
            });

            it('should return false for single component without write permissions', () => {
                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue({
                        type: ComponentType.Processor,
                        permissions: { canWrite: false }
                    }),
                    classed: jest.fn().mockReturnValue(true)
                };

                const result = clearBulletinsCondition(mockSelection);

                expect(result).toBe(false);
            });

            it('should return true for process group with bulletins (permissions not required)', () => {
                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue({
                        type: ComponentType.ProcessGroup
                    }),
                    classed: jest.fn().mockReturnValue(true)
                };

                const result = clearBulletinsCondition(mockSelection);

                expect(result).toBe(true);
                expect(mockSelection.classed).toHaveBeenCalledWith('has-bulletins');
            });

            it('should return false for single component without bulletins', () => {
                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue({
                        type: ComponentType.Processor,
                        permissions: { canWrite: true }
                    }),
                    classed: jest.fn().mockReturnValue(false)
                };

                const result = clearBulletinsCondition(mockSelection);

                expect(result).toBe(false);
            });

            it('should return false for multiple component selection', () => {
                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(2)
                };

                const result = clearBulletinsCondition(mockSelection);

                expect(result).toBe(false);
            });
        });

        describe('action', () => {
            let mockD3SelectAll: jest.SpyInstance;

            beforeEach(() => {
                mockD3SelectAll = jest.spyOn(d3, 'selectAll');
            });

            it('should dispatch clearBulletinsForProcessGroup for empty selection when bulletins exist', () => {
                const mockTimestamp = '2023-10-07T12:00:00.000Z';
                const mockEmptySelection = {
                    empty: jest.fn().mockReturnValue(true)
                };

                // Mock d3.selectAll to return components with bulletins
                const mockEach = jest.fn((callback: (d: any) => void) => {
                    // Simulate calling the callback with mock data that has bulletins
                    callback({
                        bulletins: [
                            { timestampIso: '2023-10-07T12:00:00.000Z', bulletin: { timestamp: '12:00:00 UTC' } }
                        ]
                    });
                });

                mockD3SelectAll.mockReturnValue({
                    each: mockEach
                } as any);

                // Mock getMostRecentBulletinTimestamp to return a timestamp
                mockNiFiCommon.getMostRecentBulletinTimestamp.mockReturnValue(mockTimestamp);

                clearBulletinsAction.action(mockEmptySelection);

                expect(mockD3SelectAll).toHaveBeenCalledWith('g.component.has-bulletins');
                expect(mockNiFiCommon.getMostRecentBulletinTimestamp).toHaveBeenCalled();
                expect(store.dispatch).toHaveBeenCalledWith(
                    FlowActions.clearBulletinsForProcessGroup({
                        request: {
                            processGroupId: mockProcessGroupId,
                            fromTimestamp: mockTimestamp
                        }
                    })
                );
            });

            it('should not dispatch for empty selection if no bulletins are found', () => {
                const mockEmptySelection = {
                    empty: jest.fn().mockReturnValue(true)
                };

                // Mock d3.selectAll to return components without bulletins
                const mockEach = jest.fn((callback: (d: any) => void) => {
                    // Simulate calling the callback with mock data that has no bulletins
                    callback({ bulletins: null });
                });

                mockD3SelectAll.mockReturnValue({
                    each: mockEach
                } as any);

                // Mock getMostRecentBulletinTimestamp to return null (no bulletins)
                mockNiFiCommon.getMostRecentBulletinTimestamp.mockReturnValue(null);

                clearBulletinsAction.action(mockEmptySelection);

                expect(store.dispatch).not.toHaveBeenCalled();
            });

            it('should dispatch clearBulletinsForProcessGroup for process group selection using getMostRecentBulletinTimestamp', () => {
                const mockTimestamp = '2023-10-07T12:00:00.000Z';
                const mockBulletins = [
                    { timestampIso: '2023-10-07T12:00:00.000Z', bulletin: { timestamp: '12:00:00 UTC' } }
                ];
                const mockProcessGroupData = {
                    type: ComponentType.ProcessGroup,
                    id: 'selected-process-group-id',
                    bulletins: mockBulletins
                };

                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue(mockProcessGroupData)
                };

                mockNiFiCommon.getMostRecentBulletinTimestamp.mockReturnValue(mockTimestamp);

                clearBulletinsAction.action(mockSelection);

                expect(mockNiFiCommon.getMostRecentBulletinTimestamp).toHaveBeenCalledWith(mockBulletins);
                expect(store.dispatch).toHaveBeenCalledWith(
                    FlowActions.clearBulletinsForProcessGroup({
                        request: {
                            processGroupId: mockProcessGroupData.id,
                            fromTimestamp: mockTimestamp
                        }
                    })
                );
            });

            it('should dispatch clearBulletinForComponent for processor selection using getMostRecentBulletinTimestamp', () => {
                const mockTimestamp = '2023-10-07T12:00:00.000Z';
                const mockBulletins = [
                    { timestampIso: '2023-10-07T12:00:00.000Z', bulletin: { timestamp: '12:00:00 UTC' } }
                ];
                const mockProcessorData = {
                    type: ComponentType.Processor,
                    id: 'processor-id',
                    uri: 'processor-uri',
                    bulletins: mockBulletins
                };

                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue(mockProcessorData)
                };

                mockNiFiCommon.getMostRecentBulletinTimestamp.mockReturnValue(mockTimestamp);

                clearBulletinsAction.action(mockSelection);

                expect(mockNiFiCommon.getMostRecentBulletinTimestamp).toHaveBeenCalledWith(mockBulletins);
                expect(store.dispatch).toHaveBeenCalledWith(
                    FlowActions.clearBulletinsForComponent({
                        request: {
                            uri: mockProcessorData.uri,
                            fromTimestamp: mockTimestamp,
                            componentId: mockProcessorData.id,
                            componentType: mockProcessorData.type
                        }
                    })
                );
            });

            it('should dispatch clearBulletinForComponent for input port selection using getMostRecentBulletinTimestamp', () => {
                const mockTimestamp = '2023-10-07T12:00:00.000Z';
                const mockBulletins = [
                    { timestampIso: '2023-10-07T12:00:00.000Z', bulletin: { timestamp: '12:00:00 UTC' } }
                ];
                const mockInputPortData = {
                    type: ComponentType.InputPort,
                    id: 'input-port-id',
                    uri: 'input-port-uri',
                    bulletins: mockBulletins
                };

                const mockSelection = {
                    empty: jest.fn().mockReturnValue(false),
                    size: jest.fn().mockReturnValue(1),
                    datum: jest.fn().mockReturnValue(mockInputPortData)
                };

                mockNiFiCommon.getMostRecentBulletinTimestamp.mockReturnValue(mockTimestamp);

                clearBulletinsAction.action(mockSelection);

                expect(mockNiFiCommon.getMostRecentBulletinTimestamp).toHaveBeenCalledWith(mockBulletins);
                expect(store.dispatch).toHaveBeenCalledWith(
                    FlowActions.clearBulletinsForComponent({
                        request: {
                            uri: mockInputPortData.uri,
                            fromTimestamp: mockTimestamp,
                            componentId: mockInputPortData.id,
                            componentType: mockInputPortData.type
                        }
                    })
                );
            });
        });
    });
});
