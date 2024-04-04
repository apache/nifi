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

import { ActionDetails } from './action-details.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ActionEntity } from '../../../state/flow-configuration-history-listing';

describe('ActionDetails', () => {
    let component: ActionDetails;
    let fixture: ComponentFixture<ActionDetails>;
    const data: ActionEntity = {
        id: 276,
        timestamp: '02/12/2024 12:52:54 EST',
        sourceId: '9e721628-018d-1000-38cc-5ea304d451c7',
        canRead: true,
        action: {
            id: 276,
            userIdentity: 'test',
            timestamp: '02/12/2024 12:52:54 EST',
            sourceId: '9e721628-018d-1000-38cc-5ea304d451c7',
            sourceName: 'dummy',
            sourceType: 'ProcessGroup',
            operation: 'Remove'
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ActionDetails],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(ActionDetails);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
