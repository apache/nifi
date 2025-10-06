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

import { CreateReportingTask } from './create-reporting-task.component';
import { MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialExtensionsTypesState } from '../../../../../state/extension-types/extension-types.reducer';
import { extensionTypesFeatureKey } from '../../../../../state/extension-types';
import { initialState as initialReportingTasksState } from '../../../state/reporting-tasks/reporting-tasks.reducer';
import { reportingTasksFeatureKey } from '../../../state/reporting-tasks';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { MockComponent } from 'ng-mocks';
import { DocumentedType } from '../../../../../state/shared';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { of } from 'rxjs';
import { initialState as initialErrorState } from '../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../state/error';

describe('CreateReportingTask', () => {
    let component: CreateReportingTask;
    let fixture: ComponentFixture<CreateReportingTask>;

    const reportingTaskTypes: DocumentedType[] = [
        {
            type: 'org.apache.nifi.reporting.azure.loganalytics.AzureLogAnalyticsProvenanceReportingTask',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-azure-nar',
                version: '2.0.0-SNAPSHOT'
            },
            description: 'Publishes Provenance events to to a Azure Log Analytics workspace.',
            restricted: false,
            tags: ['provenace', 'log analytics', 'reporting', 'azure']
        }
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                CreateReportingTask,
                NoopAnimationsModule,
                MatIconTestingModule,
                MockComponent(ExtensionCreation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [extensionTypesFeatureKey]: initialExtensionsTypesState,
                        settings: {
                            [reportingTasksFeatureKey]: initialReportingTasksState
                        }
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateReportingTask);
        component = fixture.componentInstance;
        component.reportingTaskTypes$ = of(reportingTaskTypes);
        component.reportingTaskTypesLoadingStatus$ = of('success');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
