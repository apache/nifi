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

import { TestBed, fakeAsync, tick } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { of, throwError } from 'rxjs';
import { FlowDiffDialog, FlowDiffDialogData } from './flow-diff-dialog';
import { RegistryService } from '../../../../../service/registry.service';
import { FlowComparisonEntity } from '../../../../../state/flow';
import { VersionedFlowSnapshotMetadata } from '../../../../../../../state/shared';
import { By } from '@angular/platform-browser';
import { provideMockStore } from '@ngrx/store/testing';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';

describe('FlowDiffDialog', () => {
    const versions: VersionedFlowSnapshotMetadata[] = [
        {
            bucketIdentifier: 'bucket',
            flowIdentifier: 'flow',
            version: '2',
            timestamp: 1712171233843,
            author: 'nifi',
            comments: 'Second version'
        },
        {
            bucketIdentifier: 'bucket',
            flowIdentifier: 'flow',
            version: '1',
            timestamp: 1712076498414,
            author: 'nifi',
            comments: 'Initial version'
        }
    ];

    const baseDialogData: FlowDiffDialogData = {
        versionControlInformation: {
            groupId: 'group-id',
            registryId: 'registry-id',
            registryName: 'registry',
            bucketId: 'bucket',
            bucketName: 'bucket',
            flowId: 'flow',
            flowName: 'Sample Flow',
            flowDescription: '',
            version: '2',
            state: 'UP_TO_DATE',
            stateExplanation: '',
            branch: null
        },
        versions,
        currentVersion: '2',
        selectedVersion: '1'
    };

    const comparison: FlowComparisonEntity = {
        componentDifferences: [
            {
                componentType: 'Processor',
                componentId: 'processor',
                processGroupId: 'group-id',
                componentName: 'GenerateFlowFile',
                differences: [
                    {
                        differenceType: 'Property Modified',
                        difference: 'Scheduling period changed'
                    }
                ]
            }
        ]
    };

    let getFlowDiffSpy: jest.Mock;

    function configureTestingModule(dialogData: FlowDiffDialogData = baseDialogData) {
        getFlowDiffSpy = jest.fn().mockReturnValue(of(comparison));

        TestBed.configureTestingModule({
            imports: [FlowDiffDialog, NoopAnimationsModule],
            providers: [
                {
                    provide: RegistryService,
                    useValue: {
                        getFlowDiff: getFlowDiffSpy
                    }
                },
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: dialogData
                },
                provideMockStore({
                    selectors: [
                        {
                            selector: selectTimeOffset,
                            value: 0
                        }
                    ]
                })
            ]
        }).compileComponents();
    }

    it('should load the initial diff when opened', fakeAsync(() => {
        configureTestingModule();

        const fixture = TestBed.createComponent(FlowDiffDialog);
        fixture.detectChanges();
        tick(250);
        fixture.detectChanges();

        expect(getFlowDiffSpy).toHaveBeenCalledWith(
            'registry-id',
            'bucket',
            'flow',
            '2',
            '1',
            null
        );

        const title = fixture.debugElement.query(By.css('h2[mat-dialog-title]'));
        expect(title.nativeElement.textContent.trim()).toBe('Flow Version Diff - Sample Flow');

        const rows = fixture.debugElement.queryAll(By.css('[data-qa="flow-diff-table"] tbody tr'));
        expect(rows.length).toBe(1);

        const summaryItems = fixture.debugElement.queryAll(By.css('[data-qa="flow-diff-message"] li'));
        expect(summaryItems.length).toBe(2);
        expect(summaryItems[0].nativeElement.textContent).toContain('Version 2');
        expect(summaryItems[1].nativeElement.textContent).toContain('Version 1');
    }));

    it('should show empty state when there are no differences', fakeAsync(() => {
        const emptyComparison: FlowComparisonEntity = {
            componentDifferences: []
        };
        configureTestingModule();
        getFlowDiffSpy.mockReturnValue(of(emptyComparison));

        const fixture = TestBed.createComponent(FlowDiffDialog);
        fixture.detectChanges();
        tick(250);
        fixture.detectChanges();

        const emptyMessage = fixture.debugElement.query(By.css('[data-qa="flow-diff-empty"]'));
        expect(emptyMessage).toBeTruthy();
    }));

    it('should display an error when the diff request fails', fakeAsync(() => {
        configureTestingModule();
        getFlowDiffSpy.mockReturnValue(throwError(() => new Error('Failed')));

        const fixture = TestBed.createComponent(FlowDiffDialog);
        fixture.detectChanges();
        tick(250);
        fixture.detectChanges();

        const errorMessage = fixture.debugElement.query(By.css('[data-qa="flow-diff-error"]'));
        expect(errorMessage).toBeTruthy();
        expect(errorMessage.nativeElement.textContent.trim()).toBe('Unable to retrieve version differences.');
    }));
});
