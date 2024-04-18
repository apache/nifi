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

import { ProvenanceSearchDialog } from './provenance-search-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatNativeDateModule } from '@angular/material/core';
import { ProvenanceSearchDialogRequest } from '../../../state/provenance-event-listing';

describe('ProvenanceSearchDialog', () => {
    let component: ProvenanceSearchDialog;
    let fixture: ComponentFixture<ProvenanceSearchDialog>;

    const data: ProvenanceSearchDialogRequest = {
        timeOffset: -18000000,
        clusterNodes: [],
        options: {
            searchableFields: [
                {
                    id: 'EventType',
                    field: 'eventType',
                    label: 'Event Type',
                    type: 'STRING'
                },
                {
                    id: 'FlowFileUUID',
                    field: 'uuid',
                    label: 'FlowFile UUID',
                    type: 'STRING'
                },
                {
                    id: 'Filename',
                    field: 'filename',
                    label: 'Filename',
                    type: 'STRING'
                },
                {
                    id: 'ProcessorID',
                    field: 'processorId',
                    label: 'Component ID',
                    type: 'STRING'
                },
                {
                    id: 'Relationship',
                    field: 'relationship',
                    label: 'Relationship',
                    type: 'STRING'
                }
            ]
        },
        currentRequest: {
            incrementalResults: false,
            maxResults: 1000,
            summarize: true
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ProvenanceSearchDialog, NoopAnimationsModule, MatNativeDateModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(ProvenanceSearchDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
