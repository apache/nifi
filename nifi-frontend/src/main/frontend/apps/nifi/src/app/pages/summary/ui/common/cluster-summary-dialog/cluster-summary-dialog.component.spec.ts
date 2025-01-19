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

import { ClusterSummaryDialog } from './cluster-summary-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialComponentClusterStatusState } from '../../../state/component-cluster-status/component-cluster-status.reducer';
import { ComponentClusterStatusRequest, ComponentClusterStatusState } from '../../../state/component-cluster-status';
import { ComponentType } from '@nifi/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('ClusterSummaryDialog', () => {
    let component: ClusterSummaryDialog;
    let fixture: ComponentFixture<ClusterSummaryDialog>;
    const data: ComponentClusterStatusRequest = {
        id: 'id',
        componentType: ComponentType.Processor
    };
    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ClusterSummaryDialog, MatDialogModule, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({
                    initialState: {
                        ...initialComponentClusterStatusState,
                        clusterStatus: {
                            canRead: true
                        }
                    } as ComponentClusterStatusState
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ClusterSummaryDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
