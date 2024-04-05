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

import { FlowFileDialog } from './flowfile-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { FlowFileDialogRequest } from '../../../state/queue-listing';

describe('FlowFileDialog', () => {
    let component: FlowFileDialog;
    let fixture: ComponentFixture<FlowFileDialog>;

    const data: FlowFileDialogRequest = {
        flowfile: {
            uri: 'https://localhost:4200/nifi-api/flowfile-queues/eea858d0-018c-1000-57fe-66ba110b3bcb/flowfiles/fc165889-3493-404c-9895-62d49a06801b',
            uuid: 'fc165889-3493-404c-9895-62d49a06801b',
            filename: '93a06a31-6b50-4d04-9b45-6a2a4a5e2dd1',
            size: 0,
            queuedDuration: 172947006,
            lineageDuration: 172947006,
            penaltyExpiresIn: 0,
            attributes: {
                path: './',
                filename: '93a06a31-6b50-4d04-9b45-6a2a4a5e2dd1',
                uuid: 'fc165889-3493-404c-9895-62d49a06801b'
            },
            penalized: false
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [FlowFileDialog, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(FlowFileDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
