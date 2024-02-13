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

import { ProvenanceEventDialog } from './provenance-event-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('ProvenanceEventDialog', () => {
    let component: ProvenanceEventDialog;
    let fixture: ComponentFixture<ProvenanceEventDialog>;

    const data: any = {
        event: {
            id: '67231',
            eventId: 67231,
            eventTime: '12/06/2023 13:24:14.934 EST',
            lineageDuration: 80729691,
            eventType: 'DROP',
            flowFileUuid: '6908fd9d-9168-4da5-a92b-0414cdb5e3bc',
            fileSize: '0 bytes',
            fileSizeBytes: 0,
            groupId: '36d207b9-018c-1000-6f5a-1b02d4517a78',
            componentId: '36d293df-018c-1000-b438-9e0cd664f5aa',
            componentType: 'Connection',
            componentName: 'success',
            attributes: [
                {
                    name: 'filename',
                    value: '6908fd9d-9168-4da5-a92b-0414cdb5e3bc',
                    previousValue: '6908fd9d-9168-4da5-a92b-0414cdb5e3bc'
                },
                {
                    name: 'path',
                    value: './',
                    previousValue: './'
                },
                {
                    name: 'uuid',
                    value: '6908fd9d-9168-4da5-a92b-0414cdb5e3bc',
                    previousValue: '6908fd9d-9168-4da5-a92b-0414cdb5e3bc'
                }
            ],
            parentUuids: [],
            childUuids: [],
            details: 'FlowFile Queue emptied by admin',
            contentEqual: false,
            inputContentAvailable: false,
            outputContentAvailable: false,
            outputContentClaimFileSize: '0 bytes',
            outputContentClaimFileSizeBytes: 0,
            replayAvailable: true,
            sourceConnectionIdentifier: '36d293df-018c-1000-b438-9e0cd664f5aa'
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ProvenanceEventDialog, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(ProvenanceEventDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
