/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { ChangeVersionDialog } from './change-version-dialog';
import { ChangeVersionDialogRequest } from '../../../../../state/flow';
import { VersionedFlowSnapshotMetadata } from '../../../../../../../state/shared';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { canvasFeatureKey } from '../../../../../state';
import { flowFeatureKey } from '../../../../../state/flow';
import { Sort } from '@angular/material/sort';

interface SetupOptions {
    dialogData?: ChangeVersionDialogRequest;
    timeOffset?: number;
}

describe('ChangeVersionDialog', () => {
    // Mock data factory
    function createMockDialogData(overrides: Partial<ChangeVersionDialogRequest> = {}): ChangeVersionDialogRequest {
        return {
            processGroupId: '80f86c6f-018e-1000-68e5-17d68c402f4c',
            revision: {
                clientId: '8f36f627-d927-4a37-a1f2-a6deb9b91e50',
                version: 31
            },
            versionControlInformation: {
                groupId: '80f86c6f-018e-1000-68e5-17d68c402f4c',
                registryId: '80441509-018e-1000-12b2-d70361a7f661',
                registryName: 'Local Registry',
                bucketId: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                bucketName: 'RAG',
                flowId: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
                flowName: 'sdaf',
                flowDescription: '',
                version: '2',
                state: 'UP_TO_DATE',
                stateExplanation: 'Flow version is current'
            },
            versions: [
                {
                    versionedFlowSnapshotMetadata: {
                        bucketIdentifier: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                        flowIdentifier: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
                        version: '2',
                        timestamp: 1712171233843,
                        author: 'anonymous',
                        comments: ''
                    },
                    registryId: '80441509-018e-1000-12b2-d70361a7f661'
                },
                {
                    versionedFlowSnapshotMetadata: {
                        bucketIdentifier: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                        flowIdentifier: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
                        version: '1',
                        timestamp: 1712076498414,
                        author: 'anonymous',
                        comments: ''
                    },
                    registryId: '80441509-018e-1000-12b2-d70361a7f661'
                }
            ],
            ...overrides
        };
    }

    async function setup(options: SetupOptions = {}) {
        const dialogData = options.dialogData || createMockDialogData();
        const timeOffset = options.timeOffset || 0;

        await TestBed.configureTestingModule({
            imports: [ChangeVersionDialog, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: dialogData },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState
                        }
                    },
                    selectors: [
                        {
                            selector: selectTimeOffset,
                            value: timeOffset
                        }
                    ]
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(ChangeVersionDialog);
        const component = fixture.componentInstance;
        fixture.detectChanges();

        return { fixture, component, dialogData };
    }

    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();

        expect(component).toBeTruthy();
    });

    describe('Component Initialization', () => {
        it('should initialize with sorted flow versions', async () => {
            const { component } = await setup();

            expect(component.dataSource.data).toHaveLength(2);
            expect(component.dataSource.data[0].version).toBe('2'); // Most recent first
            expect(component.dataSource.data[1].version).toBe('1');
        });

        it('should select the first (most recent) version by default', async () => {
            const { component } = await setup();

            expect(component.selectedFlowVersion).toBeTruthy();
            expect(component.selectedFlowVersion?.version).toBe('2');
        });

        it('should set version control information from dialog data', async () => {
            const { component, dialogData } = await setup();

            expect(component.versionControlInformation).toEqual(dialogData.versionControlInformation);
        });
    });

    describe('Current Version Detection', () => {
        it('should identify current version correctly', async () => {
            const { component } = await setup();
            const currentVersion = component.dataSource.data.find((v) => v.version === '2');
            const oldVersion = component.dataSource.data.find((v) => v.version === '1');

            expect(component.isCurrentVersion(currentVersion!)).toBe(true);
            expect(component.isCurrentVersion(oldVersion!)).toBe(false);
        });

        it('should display current version icon in template', async () => {
            const { fixture } = await setup();
            const currentVersionIcon = fixture.debugElement.query(By.css('[data-qa="current-version-icon"]'));

            expect(currentVersionIcon).toBeTruthy();
            expect(currentVersionIcon.attributes['tooltipInputData']).toBe('Currently installed version');
            expect(currentVersionIcon.nativeElement.classList).toContain('fa-check');
        });

        it('should only show current version icon for the installed version', async () => {
            const { fixture } = await setup();
            const allIcons = fixture.debugElement.queryAll(By.css('[data-qa="current-version-icon"]'));

            // Should only have one current version icon
            expect(allIcons).toHaveLength(1);
        });
    });

    describe('Version Selection', () => {
        it('should select a version when select method is called', async () => {
            const { component } = await setup();
            const versionToSelect = component.dataSource.data[1]; // Version '1'

            component.select(versionToSelect);

            expect(component.selectedFlowVersion).toEqual(versionToSelect);
        });

        it('should identify selected version correctly', async () => {
            const { component } = await setup();
            const version1 = component.dataSource.data[0];
            const version2 = component.dataSource.data[1];

            component.select(version2);

            expect(component.isSelected(version1)).toBe(false);
            expect(component.isSelected(version2)).toBe(true);
        });

        it('should return false for isSelected when no version is selected', async () => {
            const { component } = await setup();
            component.selectedFlowVersion = null;
            const version = component.dataSource.data[0];

            expect(component.isSelected(version)).toBe(false);
        });
    });

    describe('Selection Validation', () => {
        it('should be invalid when no version is selected', async () => {
            const { component } = await setup();
            component.selectedFlowVersion = null;

            expect(component.isSelectionValid()).toBe(false);
        });

        it('should be invalid when current version is selected', async () => {
            const { component } = await setup();
            const currentVersion = component.dataSource.data.find((v) => v.version === '2');
            component.select(currentVersion!);

            expect(component.isSelectionValid()).toBe(false);
        });

        it('should be valid when different version is selected', async () => {
            const { component } = await setup();
            const differentVersion = component.dataSource.data.find((v) => v.version === '1');
            component.select(differentVersion!);

            expect(component.isSelectionValid()).toBe(true);
        });
    });

    describe('Sorting Functionality', () => {
        it('should sort by version in ascending order', async () => {
            const { component } = await setup();
            const sort: Sort = { active: 'version', direction: 'asc' };

            component.sortData(sort);

            expect(component.dataSource.data[0].version).toBe('1');
            expect(component.dataSource.data[1].version).toBe('2');
        });

        it('should sort by version in descending order', async () => {
            const { component } = await setup();
            const sort: Sort = { active: 'version', direction: 'desc' };

            component.sortData(sort);

            expect(component.dataSource.data[0].version).toBe('2');
            expect(component.dataSource.data[1].version).toBe('1');
        });

        it('should sort by timestamp in ascending order', async () => {
            const { component } = await setup();
            const sort: Sort = { active: 'created', direction: 'asc' };

            component.sortData(sort);

            expect(component.dataSource.data[0].timestamp).toBe(1712076498414); // Older timestamp first
            expect(component.dataSource.data[1].timestamp).toBe(1712171233843);
        });

        it('should sort by comments', async () => {
            const { component, dialogData } = await setup();
            // Add versions with different comments for testing
            const versionsWithComments = [
                {
                    versionedFlowSnapshotMetadata: {
                        bucketIdentifier: 'test',
                        flowIdentifier: 'test',
                        version: '3',
                        timestamp: 1712171233844,
                        author: 'test',
                        comments: 'Z comment'
                    },
                    registryId: 'test'
                },
                {
                    versionedFlowSnapshotMetadata: {
                        bucketIdentifier: 'test',
                        flowIdentifier: 'test',
                        version: '4',
                        timestamp: 1712171233845,
                        author: 'test',
                        comments: 'A comment'
                    },
                    registryId: 'test'
                }
            ];

            const testData = [...dialogData.versions, ...versionsWithComments];
            const flowVersions = testData.map((entity) => entity.versionedFlowSnapshotMetadata);
            const sort: Sort = { active: 'comments', direction: 'asc' };

            const sortedVersions = component.sortVersions(flowVersions, sort);

            expect(sortedVersions[0].comments).toBe(''); // Empty comments first
            expect(sortedVersions[2].comments).toBe('A comment');
            expect(sortedVersions[3].comments).toBe('Z comment');
        });

        it('should handle empty data array in sortVersions', async () => {
            const { component } = await setup();
            const sort: Sort = { active: 'version', direction: 'asc' };

            const result = component.sortVersions([], sort);

            expect(result).toEqual([]);
        });

        it('should handle null data in sortVersions', async () => {
            const { component } = await setup();
            const sort: Sort = { active: 'version', direction: 'asc' };

            const result = component.sortVersions(null as any, sort);

            expect(result).toEqual([]);
        });
    });

    describe('Timestamp Formatting', () => {
        it('should format timestamp correctly', async () => {
            const { component } = await setup();
            const version = component.dataSource.data[0];

            const formattedTime = component.formatTimestamp(version);

            expect(formattedTime).toBeTruthy();
            expect(typeof formattedTime).toBe('string');
        });

        it('should handle timezone offset in timestamp formatting', async () => {
            const { component } = await setup({ timeOffset: -18000000 }); // -5 hours in milliseconds
            const version = component.dataSource.data[0];

            const formattedTime = component.formatTimestamp(version);

            expect(formattedTime).toBeTruthy();
            expect(typeof formattedTime).toBe('string');
        });
    });

    describe('Version Change Event', () => {
        it('should emit changeVersion event when changeFlowVersion is called with selected version', async () => {
            const { component } = await setup();
            jest.spyOn(component.changeVersion, 'next');
            const versionToSelect = component.dataSource.data[1];
            component.select(versionToSelect);

            component.changeFlowVersion();

            expect(component.changeVersion.next).toHaveBeenCalledWith(versionToSelect);
        });

        it('should not emit changeVersion event when no version is selected', async () => {
            const { component } = await setup();
            jest.spyOn(component.changeVersion, 'next');
            component.selectedFlowVersion = null;

            component.changeFlowVersion();

            expect(component.changeVersion.next).not.toHaveBeenCalled();
        });
    });

    describe('Version Comparison', () => {
        it('should compare numeric versions correctly', async () => {
            const { component } = await setup();
            const testVersions: VersionedFlowSnapshotMetadata[] = [
                {
                    bucketIdentifier: 'test',
                    flowIdentifier: 'test',
                    version: '10',
                    timestamp: 1,
                    author: 'test',
                    comments: ''
                },
                {
                    bucketIdentifier: 'test',
                    flowIdentifier: 'test',
                    version: '2',
                    timestamp: 2,
                    author: 'test',
                    comments: ''
                }
            ];

            const sort: Sort = { active: 'version', direction: 'asc' };
            const sorted = component.sortVersions(testVersions, sort);

            expect(sorted[0].version).toBe('2');
            expect(sorted[1].version).toBe('10');
        });

        it('should compare string versions correctly', async () => {
            const { component } = await setup();
            const testVersions: VersionedFlowSnapshotMetadata[] = [
                {
                    bucketIdentifier: 'test',
                    flowIdentifier: 'test',
                    version: 'v2.0',
                    timestamp: 1,
                    author: 'test',
                    comments: ''
                },
                {
                    bucketIdentifier: 'test',
                    flowIdentifier: 'test',
                    version: 'v1.0',
                    timestamp: 2,
                    author: 'test',
                    comments: ''
                }
            ];

            const sort: Sort = { active: 'version', direction: 'asc' };
            const sorted = component.sortVersions(testVersions, sort);

            expect(sorted[0].version).toBe('v1.0');
            expect(sorted[1].version).toBe('v2.0');
        });
    });

    describe('DOM Interactions', () => {
        it('should handle row click for version selection', async () => {
            const { component, fixture } = await setup();
            jest.spyOn(component, 'select');
            const firstDataRow = fixture.debugElement.query(By.css('[data-qa="version-row"]'));

            firstDataRow.nativeElement.click();

            expect(component.select).toHaveBeenCalled();
        });

        it('should handle double click for version change', async () => {
            const { component, fixture } = await setup();
            jest.spyOn(component, 'changeFlowVersion');
            const firstDataRow = fixture.debugElement.query(By.css('[data-qa="version-row"]'));

            firstDataRow.nativeElement.dispatchEvent(new Event('dblclick'));

            expect(component.changeFlowVersion).toHaveBeenCalled();
        });

        it('should apply selected class to selected row', async () => {
            const { component, fixture } = await setup();
            const versionToSelect = component.dataSource.data[1];
            component.select(versionToSelect);
            fixture.detectChanges();

            const rows = fixture.debugElement.queryAll(By.css('[data-qa="version-row"]'));
            const selectedRow = rows.find((row) => row.classes['selected']);

            expect(selectedRow).toBeTruthy();
        });

        it('should disable change button when no valid selection', async () => {
            const { component, fixture } = await setup();
            component.selectedFlowVersion = null;
            fixture.detectChanges();

            const changeButton = fixture.debugElement.query(By.css('[data-qa="change-button"]'));

            expect(changeButton.nativeElement.disabled).toBe(true);
        });

        it('should enable change button when valid selection is made', async () => {
            const { component, fixture } = await setup();
            const differentVersion = component.dataSource.data.find((v) => v.version === '1');
            component.select(differentVersion!);
            fixture.detectChanges();

            const changeButton = fixture.debugElement.query(By.css('[data-qa="change-button"]'));

            expect(changeButton.nativeElement.disabled).toBe(false);
        });
    });
});
