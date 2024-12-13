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

import { CreateRemoteProcessGroup } from './create-remote-process-group.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CreateComponentRequest } from '../../../../../state/flow';

describe('CreateRemoteProcessGroup', () => {
    let component: CreateRemoteProcessGroup;
    let fixture: ComponentFixture<CreateRemoteProcessGroup>;

    const data: CreateComponentRequest = {
        revision: {
            clientId: 'a6482293-7fe8-43b4-8ab4-ee95b3b27721',
            version: 0
        },
        type: ComponentType.RemoteProcessGroup,
        position: {
            x: -4,
            y: -698.5
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateRemoteProcessGroup, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({ initialState }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateRemoteProcessGroup);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
