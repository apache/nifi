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

import { CanvasUtils } from './canvas-utils.service';
import { CanvasState } from '../state';
import { flowFeatureKey } from '../state/flow';
import * as fromFlow from '../state/flow/flow.reducer';
import { transformFeatureKey } from '../state/transform';
import * as fromTransform from '../state/transform/transform.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../state/flow/flow.selectors';
import { controllerServicesFeatureKey } from '../state/controller-services';
import * as fromControllerServices from '../state/controller-services/controller-services.reducer';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../state/current-user/current-user.reducer';
import { parameterFeatureKey } from '../state/parameter';
import * as fromParameter from '../state/parameter/parameter.reducer';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../state/flow-configuration/flow-configuration.reducer';
import { queueFeatureKey } from '../../queue/state';
import * as fromQueue from '../state/queue/queue.reducer';
import { flowAnalysisFeatureKey } from '../state/flow-analysis';
import * as fromFlowAnalysis from '../state/flow-analysis/flow-analysis.reducer';
import { ComponentType } from '@nifi/shared';
import * as d3 from 'd3';

describe('CanvasUtils', () => {
    let service: CanvasUtils;

    beforeEach(() => {
        const initialState: CanvasState = {
            [flowFeatureKey]: fromFlow.initialState,
            [transformFeatureKey]: fromTransform.initialState,
            [controllerServicesFeatureKey]: fromControllerServices.initialState,
            [parameterFeatureKey]: fromParameter.initialState,
            [queueFeatureKey]: fromQueue.initialState,
            [flowAnalysisFeatureKey]: fromFlowAnalysis.initialState
        };

        TestBed.configureTestingModule({
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectFlowState,
                            value: initialState[flowFeatureKey]
                        },
                        {
                            selector: selectCurrentUser,
                            value: fromUser.initialState.user
                        },
                        {
                            selector: selectFlowConfiguration,
                            value: fromFlowConfiguration.initialState.flowConfiguration
                        }
                    ]
                })
            ]
        });
        service = TestBed.inject(CanvasUtils);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('supportsStopFlowVersioning', () => {
        it('should return null if selection is non empty and version control information is missing', () => {
            const pgDatum = {
                id: '1',
                type: ComponentType.ProcessGroup,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '1',
                    name: 'Test Process Group',
                    versionControlInformation: null
                }
            };
            const selection = d3.select(document.createElement('div')).classed('process-group', true).datum(pgDatum);
            expect(service.getFlowVersionControlInformation(selection)).toBe(null);
        });

        it('should return vci if selection is non empty and version control information is present', () => {
            const versionControlInformation = {
                groupId: '1',
                registryId: '324e0ab1-0197-1000-ffff-ffffb3123c5c',
                registryName: 'ConnectorFlowRegistryClient',
                branch: 'main',
                bucketId: 'connectors',
                bucketName: 'connectors',
                flowId: 'kafka-json-sasl-topic2table-schemaev',
                flowName: 'kafka-json-sasl-topic2table-schemaev',
                version: '0.1.0-f47ff72',
                state: 'UP_TO_DATE',
                stateExplanation: 'Flow version is current'
            };
            const pgDatum = {
                id: '1',
                type: ComponentType.ProcessGroup,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '1',
                    name: 'Test Process Group',
                    versionControlInformation
                }
            };
            const selection = d3.select(document.createElement('div')).classed('process-group', true).datum(pgDatum);
            expect(service.getFlowVersionControlInformation(selection)).toBe(versionControlInformation);
        });
    });

    describe('isStoppable', () => {
        it('should return false for empty selection', () => {
            const emptySelection = d3.select(null);
            expect(service.isStoppable(emptySelection)).toBe(false);
        });

        it('should return false for multiple selections', () => {
            const div1 = document.createElement('g');
            const div2 = document.createElement('g');
            const multiSelection = d3.selectAll([div1, div2]);
            expect(service.isStoppable(multiSelection)).toBe(false);
        });

        it('should return true for process groups', () => {
            const pgDatum = {
                id: '1',
                type: ComponentType.ProcessGroup,
                permissions: { canRead: true, canWrite: true },
                operatePermissions: { canWrite: true }
            };
            const selection = d3.select(document.createElement('g')).classed('process-group', true).datum(pgDatum);
            expect(service.isStoppable(selection)).toBe(true);
        });

        it('should return false when lacking operate permissions', () => {
            const processorDatum = {
                id: '1',
                type: ComponentType.Processor,
                permissions: { canRead: true, canWrite: false },
                operatePermissions: { canWrite: false },
                status: {
                    aggregateSnapshot: { runStatus: 'Running' }
                },
                physicalState: 'RUNNING'
            };
            const selection = d3.select(document.createElement('g')).classed('processor', true).datum(processorDatum);
            expect(service.isStoppable(selection)).toBe(false);
        });

        describe('for processors', () => {
            it('should return true when runStatus is Running', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Running' }
                    },
                    physicalState: 'RUNNING'
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(true);
            });

            it('should return false when runStatus is Stopped', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Stopped' }
                    },
                    physicalState: 'STOPPED'
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return true when runStatus is Invalid and physicalState is STARTING', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Invalid' }
                    },
                    physicalState: 'STARTING'
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(true);
            });

            it('should return false when runStatus is Invalid and physicalState is STOPPED', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Invalid' }
                    },
                    physicalState: 'STOPPED'
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return false when runStatus is Invalid and physicalState is DISABLED', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Invalid' }
                    },
                    physicalState: 'DISABLED'
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return false when runStatus is Invalid and physicalState is null', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Invalid' }
                    },
                    physicalState: null
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return false when runStatus is Invalid and physicalState is undefined', () => {
                const processorDatum = {
                    id: '1',
                    type: ComponentType.Processor,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Invalid' }
                    }
                    // physicalState is undefined
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('processor', true)
                    .datum(processorDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });
        });

        describe('for input ports', () => {
            it('should return true when runStatus is Running', () => {
                const inputPortDatum = {
                    id: '1',
                    type: ComponentType.InputPort,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Running' }
                    }
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('input-port', true)
                    .datum(inputPortDatum);
                expect(service.isStoppable(selection)).toBe(true);
            });

            it('should return false when runStatus is Stopped', () => {
                const inputPortDatum = {
                    id: '1',
                    type: ComponentType.InputPort,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Stopped' }
                    }
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('input-port', true)
                    .datum(inputPortDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });
        });

        describe('for output ports', () => {
            it('should return true when runStatus is Running', () => {
                const outputPortDatum = {
                    id: '1',
                    type: ComponentType.OutputPort,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Running' }
                    }
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('output-port', true)
                    .datum(outputPortDatum);
                expect(service.isStoppable(selection)).toBe(true);
            });

            it('should return false when runStatus is Stopped', () => {
                const outputPortDatum = {
                    id: '1',
                    type: ComponentType.OutputPort,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true },
                    status: {
                        aggregateSnapshot: { runStatus: 'Stopped' }
                    }
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('output-port', true)
                    .datum(outputPortDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });
        });

        describe('for other component types', () => {
            it('should return false for connections', () => {
                const connectionDatum = {
                    id: '1',
                    type: ComponentType.Connection,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true }
                };
                const selection = d3
                    .select(document.createElement('g'))
                    .classed('connection', true)
                    .datum(connectionDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return false for labels', () => {
                const labelDatum = {
                    id: '1',
                    type: ComponentType.Label,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true }
                };
                const selection = d3.select(document.createElement('g')).classed('label', true).datum(labelDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });

            it('should return false for funnels', () => {
                const funnelDatum = {
                    id: '1',
                    type: ComponentType.Funnel,
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canWrite: true }
                };
                const selection = d3.select(document.createElement('g')).classed('funnel', true).datum(funnelDatum);
                expect(service.isStoppable(selection)).toBe(false);
            });
        });
    });
});
