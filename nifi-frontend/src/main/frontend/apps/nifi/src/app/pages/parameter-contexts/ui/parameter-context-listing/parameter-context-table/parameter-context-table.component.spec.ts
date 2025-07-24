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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ParameterContextTable } from './parameter-context-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatTableDataSource } from '@angular/material/table';
import { ParameterContextEntity } from '../../../../../state/shared';
import { Sort } from '@angular/material/sort';
import { RouterModule } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';

describe('ParameterContextTable', () => {
    let component: ParameterContextTable;
    let fixture: ComponentFixture<ParameterContextTable>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ParameterContextTable, NoopAnimationsModule, RouterModule, MatIconModule]
        });
        fixture = TestBed.createComponent(ParameterContextTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('with canRead returning true', () => {
        beforeEach(() => {
            jest.spyOn(component, 'canRead').mockReturnValue(true);
        });

        it('should format name correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    name: 'Example Name'
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatName(entity)).toEqual('Example Name');
        });

        it('should format provider correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    parameterProviderConfiguration: {
                        component: {
                            parameterGroupName: 'Example Group Name',
                            parameterProviderName: 'Example Provider'
                        }
                    }
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProvider(entity)).toEqual('Example Group Name from Example Provider');
        });

        it('should format description correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    description: 'Example Description'
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatDescription(entity)).toEqual('Example Description');
        });

        it('should format process groups correctly when there are more than 1 process group', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    boundProcessGroups: [
                        {
                            id: '1',
                            permissions: { canRead: true },
                            component: { id: '111', name: 'PG1' }
                        },
                        {
                            id: '2',
                            permissions: { canRead: true },
                            component: { id: '222', name: 'PG1' }
                        }
                    ]
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProcessGroups(entity)).toEqual('2 referencing Process Groups');
        });

        it('should format process groups correctly when there is one process group', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    boundProcessGroups: [
                        {
                            id: '1',
                            permissions: { canRead: true },
                            component: { id: '111', name: 'PG1' }
                        }
                    ]
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProcessGroups(entity)).toEqual('PG1');
        });

        it('should format process groups correctly when there are no process groups', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    boundProcessGroups: []
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProcessGroups(entity)).toEqual('');
        });

        it('should format process groups correctly when there is 1 process groups but without read permission', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    boundProcessGroups: [
                        {
                            id: '1',
                            permissions: { canRead: false },
                            component: { id: '111', name: 'PG1' }
                        }
                    ]
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProcessGroups(entity)).toEqual('1 referencing Process Group');
        });

        it.each([
            { active: 'name', direction: 'asc', expectedOrder: ['2', '1', '3'] },
            { active: 'name', direction: 'desc', expectedOrder: ['3', '1', '2'] },
            { active: 'description', direction: 'asc', expectedOrder: ['2', '1', '3'] },
            { active: 'description', direction: 'desc', expectedOrder: ['3', '1', '2'] },
            { active: 'provider', direction: 'asc', expectedOrder: ['1', '2', '3'] },
            { active: 'provider', direction: 'desc', expectedOrder: ['3', '2', '1'] },
            { active: 'process groups', direction: 'asc', expectedOrder: ['1', '2', '3'] },
            { active: 'process groups', direction: 'desc', expectedOrder: ['3', '2', '1'] }
        ])('should sort entities correctly by %s', ({ active, direction, expectedOrder }) => {
            const mockEntities = [
                {
                    id: '1',
                    component: {
                        name: 'B',
                        description: 'Second',
                        parameterProviderConfiguration: {
                            component: {
                                parameterGroupName: 'Name1'
                            }
                        },
                        boundProcessGroups: [
                            {
                                id: '1',
                                permissions: { canRead: true },
                                component: { id: '111', name: 'PG1' }
                            }
                        ]
                    }
                } as unknown as ParameterContextEntity,
                {
                    id: '2',
                    component: {
                        name: 'A',
                        description: 'First',
                        parameterProviderConfiguration: {
                            component: {
                                parameterGroupName: 'Name2'
                            }
                        },
                        boundProcessGroups: [
                            {
                                id: '2',
                                permissions: { canRead: true },
                                component: { id: '222', name: 'PG2' }
                            }
                        ]
                    }
                } as unknown as ParameterContextEntity,
                {
                    id: '3',
                    component: {
                        name: 'C',
                        description: 'Third',
                        parameterProviderConfiguration: {
                            component: {
                                parameterGroupName: 'Name3'
                            }
                        },
                        boundProcessGroups: [
                            {
                                id: '3',
                                permissions: { canRead: true },
                                component: { id: '333', name: 'PG3' }
                            }
                        ]
                    }
                } as unknown as ParameterContextEntity
            ] as ParameterContextEntity[];

            const sortEvent = { active, direction } as Sort;
            component.dataSource = new MatTableDataSource(mockEntities);

            component.sortData(sortEvent);
            const sortedData = component.dataSource.data.map((entity) => entity.id);

            expect(sortedData).toEqual(expectedOrder);
        });
    });

    describe('with canRead returning false', () => {
        beforeEach(() => {
            jest.spyOn(component, 'canRead').mockReturnValue(false);
        });

        it('should format name correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    name: 'Example Name'
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatName(entity)).toEqual('1');
        });

        it('should format provider correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    parameterProviderConfiguration: {
                        component: {
                            parameterGroupName: 'Example Group Name',
                            parameterProviderName: 'Example Provider'
                        }
                    }
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProvider(entity)).toEqual('');
        });

        it('should format description correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    description: 'Example Description'
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatDescription(entity)).toEqual('');
        });

        it('should format process groups correctly', () => {
            const entity: ParameterContextEntity = {
                id: '1',
                component: {
                    boundProcessGroups: [
                        {
                            id: '1',
                            permissions: { canRead: true },
                            component: { id: '111', name: 'PG1' }
                        },
                        {
                            id: '2',
                            permissions: { canRead: true },
                            component: { id: '222', name: 'PG1' }
                        }
                    ]
                }
            } as unknown as ParameterContextEntity;

            expect(component.formatProcessGroups(entity)).toEqual('');
        });
    });

    describe('verify parameter context actions', () => {
        const parameterContext: ParameterContextEntity = {
            id: '1',
            permissions: { canRead: true, canWrite: true },
            revision: { version: 0, clientId: '1', lastModifier: '1' },
            uri: '/nifi-api/parameter-contexts/1',
            component: {
                name: 'Test Parameter Context',
                description: 'Test Description',
                parameterProviderConfiguration: {
                    component: {
                        parameterGroupName: 'Test Group'
                    }
                },
                boundProcessGroups: []
            }
        } as unknown as ParameterContextEntity;

        it('should trigger the select action', async () => {
            jest.spyOn(component.selectParameterContext, 'next');
            component.select(parameterContext);
            expect(component.selectParameterContext.next).toHaveBeenCalledWith(parameterContext);
        });

        it('should trigger the edit action', () => {
            jest.spyOn(component.editParameterContext, 'next');
            component.editClicked(parameterContext);
            expect(component.editParameterContext.next).toHaveBeenCalledWith(parameterContext);
        });

        it('should trigger the delete action', () => {
            jest.spyOn(component.deleteParameterContext, 'next');
            component.deleteClicked(parameterContext);
            expect(component.deleteParameterContext.next).toHaveBeenCalledWith(parameterContext);
        });

        it('should trigger the manage access policies action', () => {
            jest.spyOn(component.manageAccessPolicies, 'next');
            component.manageAccessPoliciesClicked(parameterContext);
            expect(component.manageAccessPolicies.next).toHaveBeenCalledWith(parameterContext);
        });
    });
});
