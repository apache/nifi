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
import { FlowDiffDialog, FlowDiffDialogData } from './flow-diff-dialog';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { RegistryService } from '../../../../../service/registry.service';
import { ErrorContextKey, errorFeatureKey } from '../../../../../../../state/error';
import { initialState as errorInitialState } from '../../../../../../../state/error/error.reducer';
import { VersionedFlowSnapshotMetadata } from '../../../../../../../state/shared';
import { FlowComparisonEntity } from '../../../../../state/flow';
import { of, Subject, throwError } from 'rxjs';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import * as ErrorActions from '../../../../../../../state/error/error.actions';

interface SetupOptions {
    dialogData?: FlowDiffDialogData;
    registryServiceOverrides?: Partial<RegistryService>;
}

describe('FlowDiffDialog', () => {
    function createMockVersions(): VersionedFlowSnapshotMetadata[] {
        return [
            {
                bucketIdentifier: 'bucket-1',
                flowIdentifier: 'flow-1',
                version: '2',
                timestamp: 1712171233843,
                author: 'user-a',
                comments: 'Second version'
            },
            {
                bucketIdentifier: 'bucket-1',
                flowIdentifier: 'flow-1',
                version: '1',
                timestamp: 1712076498414,
                author: 'user-a',
                comments: 'Initial version'
            }
        ];
    }

    function createMockDialogData(overrides: Partial<FlowDiffDialogData> = {}): FlowDiffDialogData {
        return {
            versionControlInformation: {
                groupId: 'pg-1',
                registryId: 'reg-1',
                registryName: 'Local Registry',
                bucketId: 'bucket-1',
                bucketName: 'My Bucket',
                flowId: 'flow-1',
                flowName: 'Test Flow',
                flowDescription: '',
                version: '2',
                state: 'UP_TO_DATE',
                stateExplanation: 'Flow version is current'
            },
            versions: createMockVersions(),
            currentVersion: '2',
            selectedVersion: '1',
            errorContext: ErrorContextKey.FLOW_DIFF,
            formatTimestamp: (v: VersionedFlowSnapshotMetadata) => `formatted-${v.version}`,
            ...overrides
        };
    }

    function createMockRegistryService(overrides: Partial<RegistryService> = {}): Partial<RegistryService> {
        return {
            getFlowDiff: vi.fn().mockReturnValue(
                of({
                    componentDifferences: [
                        {
                            componentType: 'Processor',
                            componentId: 'proc-1',
                            processGroupId: 'pg-1',
                            componentName: 'GenerateFlowFile',
                            differences: [
                                {
                                    differenceType: 'Property Value Changed',
                                    difference: 'File Size changed from 0B to 1KB'
                                }
                            ]
                        }
                    ]
                })
            ),
            ...overrides
        };
    }

    async function setup(options: SetupOptions = {}) {
        const dialogData = options.dialogData || createMockDialogData();
        const mockRegistryService = createMockRegistryService(options.registryServiceOverrides);

        await TestBed.configureTestingModule({
            imports: [FlowDiffDialog, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: dialogData },
                { provide: MatDialogRef, useValue: null },
                { provide: RegistryService, useValue: mockRegistryService },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: errorInitialState
                    }
                })
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        const fixture = TestBed.createComponent(FlowDiffDialog);
        const component = fixture.componentInstance;
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();

        return { fixture, component, dialogData, mockRegistryService, store, dispatchSpy };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();

        expect(component).toBeTruthy();
    });

    describe('Component Initialization', () => {
        it('should set flow name from version control information', async () => {
            const { component } = await setup();

            expect(component.flowName).toBe('Test Flow');
        });

        it('should populate version options from provided versions', async () => {
            const { component } = await setup();

            expect(component.versionOptions).toHaveLength(2);
            expect(component.versionOptions).toContain('2');
            expect(component.versionOptions).toContain('1');
        });

        it('should initialize form controls with current and selected versions', async () => {
            const { component } = await setup();

            expect(component.currentVersionControl.value).toBe('2');
            expect(component.selectedVersionControl.value).toBe('1');
        });

        it('should set comparison summary on initialization', async () => {
            const { component } = await setup();

            expect(component.comparisonSummary).toHaveLength(2);
            expect(component.comparisonSummary[0].label).toBe('Current Version');
            expect(component.comparisonSummary[0].version).toBe('2');
            expect(component.comparisonSummary[1].label).toBe('Selected Version');
            expect(component.comparisonSummary[1].version).toBe('1');
        });
    });

    describe('Flow Diff Loading', () => {
        it('should call registry service to load diff on initialization', async () => {
            const { mockRegistryService } = await setup();

            expect(mockRegistryService.getFlowDiff).toHaveBeenCalledWith('reg-1', 'bucket-1', 'flow-1', '2', '1', null);
        });

        it('should populate data source with diff rows', async () => {
            const { component } = await setup();

            expect(component.dataSource.data.length).toBeGreaterThan(0);
            expect(component.dataSource.data[0].componentName).toBe('GenerateFlowFile');
            expect(component.dataSource.data[0].changeType).toBe('Property Value Changed');
            expect(component.dataSource.data[0].difference).toBe('File Size changed from 0B to 1KB');
        });

        it('should set noDifferences when comparison returns empty', async () => {
            const { component } = await setup({
                registryServiceOverrides: {
                    getFlowDiff: vi.fn().mockReturnValue(of({ componentDifferences: [] }))
                }
            });

            expect(component.noDifferences).toBe(true);
            expect(component.dataSource.data).toHaveLength(0);
        });

        it('should show the loading spinner while the diff request is in flight and hide it once it completes', async () => {
            const diffSubject = new Subject<FlowComparisonEntity>();
            const { component, fixture } = await setup({
                registryServiceOverrides: {
                    getFlowDiff: vi.fn().mockReturnValue(diffSubject.asObservable())
                }
            });

            expect(component.isLoading).toBe(true);
            expect(fixture.nativeElement.querySelector('[data-qa="flow-diff-loading"]')).toBeTruthy();

            diffSubject.next({ componentDifferences: [] });
            diffSubject.complete();

            expect(component.isLoading).toBe(false);
        });

        it('should handle error when loading diff', async () => {
            const { component, dispatchSpy } = await setup({
                registryServiceOverrides: {
                    getFlowDiff: vi.fn().mockReturnValue(throwError(() => new Error('Network error')))
                }
            });

            expect(component.hasError).toBe(true);
            expect(component.dataSource.data).toHaveLength(0);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ErrorActions.addBannerError({
                    errorContext: {
                        context: ErrorContextKey.FLOW_DIFF,
                        errors: ['Unable to retrieve version differences.']
                    }
                })
            );
        });

        it('should clear banner errors for the flow diff context when starting a comparison', async () => {
            const { dispatchSpy } = await setup();

            expect(dispatchSpy).toHaveBeenCalledWith(
                ErrorActions.clearBannerErrors({ context: ErrorContextKey.FLOW_DIFF })
            );
        });

        it('should not fetch diff and should clear the summary when both versions are equal', async () => {
            const { component, mockRegistryService } = await setup({
                dialogData: createMockDialogData({ currentVersion: '2', selectedVersion: '2' })
            });

            expect(mockRegistryService.getFlowDiff).not.toHaveBeenCalled();
            expect(component.comparisonSummary).toHaveLength(0);
        });
    });

    describe('Version Option Formatting', () => {
        it('should format version option with timestamp', async () => {
            const { component } = await setup();

            const formatted = component.formatVersionOption('2');

            expect(formatted).toContain('2');
            expect(formatted).toContain('formatted-2');
        });

        it('should truncate long version strings', async () => {
            const dialogData = createMockDialogData({
                versions: [
                    {
                        bucketIdentifier: 'bucket-1',
                        flowIdentifier: 'flow-1',
                        version: 'very-long-version-string',
                        timestamp: 1712171233843,
                        author: 'user-a',
                        comments: ''
                    }
                ],
                currentVersion: 'very-long-version-string',
                selectedVersion: 'very-long-version-string'
            });
            const { component } = await setup({ dialogData });

            const formatted = component.formatVersionOption('very-long-version-string');

            expect(formatted).toContain('very-...');
        });
    });

    describe('Sorting', () => {
        it('should sort data when sortData is called', async () => {
            const mockService = createMockRegistryService({
                getFlowDiff: vi.fn().mockReturnValue(
                    of({
                        componentDifferences: [
                            {
                                componentType: 'Processor',
                                componentId: 'proc-1',
                                processGroupId: 'pg-1',
                                componentName: 'Bravo',
                                differences: [{ differenceType: 'Added', difference: 'Added component' }]
                            },
                            {
                                componentType: 'Processor',
                                componentId: 'proc-2',
                                processGroupId: 'pg-1',
                                componentName: 'Alpha',
                                differences: [{ differenceType: 'Removed', difference: 'Removed component' }]
                            }
                        ]
                    })
                )
            });
            const { component } = await setup({ registryServiceOverrides: mockService });

            component.sortData({ active: 'componentName', direction: 'asc' });

            expect(component.sort.active).toBe('componentName');
            expect(component.sort.direction).toBe('asc');
            expect(component.dataSource.data[0].componentName).toBe('Alpha');
            expect(component.dataSource.data[1].componentName).toBe('Bravo');
        });
    });

    describe('Filtering', () => {
        beforeEach(() => vi.useFakeTimers());
        afterEach(() => vi.useRealTimers());

        it('should filter data source after debounce', async () => {
            const { component } = await setup();
            const initialCount = component.dataSource.filteredData.length;
            expect(initialCount).toBeGreaterThan(0);

            component.filterControl.setValue('nonexistent-term-xyz');
            vi.advanceTimersByTime(200);

            expect(component.dataSource.filter).toBe('nonexistent-term-xyz');
            expect(component.dataSource.filteredData.length).toBe(0);
        });

        it('should show matching rows when filter matches', async () => {
            const { component } = await setup();

            component.filterControl.setValue('GenerateFlowFile');
            vi.advanceTimersByTime(200);

            expect(component.dataSource.filteredData.length).toBe(1);
            expect(component.dataSource.filteredData[0].componentName).toBe('GenerateFlowFile');
        });
    });
});