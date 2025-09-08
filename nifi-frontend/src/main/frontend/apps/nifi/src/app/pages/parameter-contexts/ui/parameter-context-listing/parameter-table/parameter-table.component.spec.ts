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

import { ParameterItem, ParameterTable } from './parameter-table.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/parameter-context-listing/parameter-context-listing.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditParameterResponse } from '../../../../../state/shared';
import { Observable, of } from 'rxjs';
import { Parameter } from '@nifi/shared';

describe('ParameterTable', () => {
    let component: ParameterTable;
    let fixture: ComponentFixture<ParameterTable>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ParameterTable, NoopAnimationsModule],
            providers: [provideMockStore({ initialState })]
        });
        fixture = TestBed.createComponent(ParameterTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should handle no parameters with no edits', () => {
        component.writeValue([]);
        expect(component.serializeParameters()).toEqual([]);
    });

    it('should handle no parameters with one added', () => {
        component.writeValue([]);

        const parameter: Parameter = {
            name: 'param',
            value: 'value',
            description: 'asdf',
            sensitive: false
        };
        component.createNewParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter,
                valueChanged: true
            });
        };
        component.newParameterClicked();

        expect(component.serializeParameters()).toEqual([{ parameter }]);
    });

    it('should handle no parameters with one added and then edited', () => {
        component.writeValue([]);

        const parameter: Parameter = {
            name: 'param',
            value: 'value',
            description: 'asdf',
            sensitive: false
        };
        component.createNewParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter,
                valueChanged: true
            });
        };
        component.newParameterClicked();

        const parameterItem: ParameterItem = component.dataSource.data[0];
        expect(parameterItem.originalEntity.parameter).toEqual(parameter);

        const description = 'updated description';
        component.editParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter: {
                    name: 'param',
                    value: null,
                    sensitive: false,
                    description
                },
                valueChanged: false
            });
        };
        component.editClicked(parameterItem);

        expect(component.serializeParameters()).toEqual([
            {
                parameter: {
                    ...parameter,
                    description
                }
            }
        ]);
    });

    describe('isOverridden', () => {
        it('should consider non inherited or modified as not overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.isOverridden(item)).toBe(false);
        });

        it('should consider inherited but not modified as not overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.isOverridden(item)).toBe(false);
        });

        it('should consider non inherited modified as not overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.isOverridden(item)).toBe(false);
        });

        it('should consider inherited modified as overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.isOverridden(item)).toBe(true);
        });
    });

    describe('canOverridden', () => {
        it('should consider non inherited or modified as not able overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });

        it('should consider inherited but not modified as able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(true);
        });

        it('should consider non inherited and modified as not able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });

        it('should consider inherited and modified as not able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });
    });

    describe('canOverride', () => {
        it('should consider non inherited or modified as not able overridden', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });

        it('should consider inherited but not modified as able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(true);
        });

        it('should consider non inherited and modified as not able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: false
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });

        it('should consider inherited and modified as not able to override', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        inherited: true
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.canOverride(item)).toBe(false);
        });
    });

    describe('canEdit', () => {
        it('should consider inherited and not modified as unable to edit', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.canEdit(item)).toBe(false);
        });

        it('should consider inherited and modified as able to edit', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.canEdit(item)).toBe(true);
        });

        it('should consider not inherited and not modified as able to edit', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.canEdit(item)).toBe(true);
        });
    });

    describe('getInheritedParameterMessage', () => {
        it('should return empty string for non inherited parameter', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.getInheritedParameterMessage(item)).toBe('');
        });

        it('should return generic message for inherited parameter with no permissions', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true,
                        parameterContext: {
                            id: 'contextId',
                            permissions: {
                                canRead: false,
                                canWrite: false
                            }
                        }
                    },
                    canWrite: true
                }
            };
            expect(component.getInheritedParameterMessage(item)).toBe(
                'This parameter is inherited from another Parameter Context.'
            );
        });

        it('should return parameter context specific message for inherited parameter with permissions', () => {
            const name = 'contextName';
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true,
                        parameterContext: {
                            id: 'contextId',
                            permissions: {
                                canRead: true,
                                canWrite: false
                            },
                            component: {
                                id: 'contextId',
                                name
                            }
                        }
                    },
                    canWrite: true
                }
            };
            expect(component.getInheritedParameterMessage(item)).toBe(`This parameter is inherited from: ${name}`);
        });
    });

    describe('isVisible', () => {
        it('should consider deleted as not visible', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: true,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.isVisible(item)).toBe(false);
        });

        it('should consider not deleted and as visible', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: false
                    },
                    canWrite: true
                }
            };
            expect(component.isVisible(item)).toBe(true);
        });

        it('should show inherited parameters', () => {
            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.isVisible(item)).toBe(true);
        });

        it('should show not inherited parameters', () => {
            component.showInheritedParameters = false;

            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                }
            };
            expect(component.isVisible(item)).toBe(false);
        });

        it('should show overridden inherited parameters', () => {
            component.showInheritedParameters = false;

            const item: ParameterItem = {
                added: false,
                dirty: false,
                deleted: false,
                originalEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value',
                        description: 'asdf',
                        sensitive: false,
                        provided: false,
                        inherited: true
                    },
                    canWrite: true
                },
                updatedEntity: {
                    parameter: {
                        name: 'param',
                        value: 'value2',
                        description: 'asdf',
                        sensitive: false
                    },
                    canWrite: true
                }
            };
            expect(component.isVisible(item)).toBe(true);
        });
    });
});
