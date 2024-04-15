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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChangeComponentVersionDialog } from './change-component-version-dialog';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatFormFieldModule } from '@angular/material/form-field';
import { OpenChangeComponentVersionDialogRequest } from '../../../state/shared';

describe('ChangeComponentVersionDialog', () => {
    let component: ChangeComponentVersionDialog;
    let fixture: ComponentFixture<ChangeComponentVersionDialog>;
    const data: OpenChangeComponentVersionDialogRequest = {
        fetchRequest: {
            id: 'd3acc2a0-018e-1000-e4c1-1fd32cb579b6',
            uri: 'https://localhost:4200/nifi-api/processors/d3acc2a0-018e-1000-e4c1-1fd32cb579b6',
            revision: {
                clientId: 'e23146d1-018e-1000-9d09-6a3c09c72420',
                version: 8
            },
            type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-standard-nar',
                version: '2.0.0-M2'
            }
        },
        componentVersions: [
            {
                type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-standard-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                description:
                    'This processor creates FlowFiles with random data or custom content. GenerateFlowFile is useful for load testing, configuration, and simulation. Also see DuplicateFlowFile for additional load testing.',
                restricted: false,
                tags: ['random', 'test', 'load', 'generate']
            },
            {
                type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-standard-nar',
                    version: '2.0.0-M2'
                },
                description:
                    'This processor creates FlowFiles with random data or custom content. GenerateFlowFile is useful for load testing, configuration, and simulation. Also see DuplicateFlowFile for additional load testing.',
                restricted: false,
                tags: ['random', 'test', 'load', 'generate']
            }
        ]
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ChangeComponentVersionDialog, MatDialogModule, NoopAnimationsModule, MatFormFieldModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        }).compileComponents();

        fixture = TestBed.createComponent(ChangeComponentVersionDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
