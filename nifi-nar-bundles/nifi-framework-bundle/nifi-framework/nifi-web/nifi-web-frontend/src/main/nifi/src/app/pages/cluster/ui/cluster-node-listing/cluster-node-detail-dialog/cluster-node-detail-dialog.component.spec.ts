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

import { ClusterNodeDetailDialog } from './cluster-node-detail-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ClusterNode } from '../../../state/cluster-listing';

describe('ClusterNodeDetailDialog', () => {
    let component: ClusterNodeDetailDialog;
    let fixture: ComponentFixture<ClusterNodeDetailDialog>;
    const data: ClusterNode = {
        nodeId: '53b34e14-e9bb-455c-ac5d-9dda95b63bb5',
        address: 'localhost',
        apiPort: 8443,
        status: 'CONNECTED',
        heartbeat: '04/18/2024 11:12:32 EDT',
        roles: ['Primary Node', 'Cluster Coordinator'],
        activeThreadCount: 0,
        queued: '0 / 0 bytes',
        events: [
            {
                timestamp: '04/18/2024 11:11:17 EDT',
                category: 'INFO',
                message: 'Received first heartbeat from connecting node. Node connected.'
            },
            {
                timestamp: '04/18/2024 11:11:11 EDT',
                category: 'INFO',
                message: 'Connection requested from existing node. Setting status to connecting.'
            },
            {
                timestamp: '04/18/2024 11:11:01 EDT',
                category: 'INFO',
                message: 'Connection requested from existing node. Setting status to connecting.'
            }
        ],
        nodeStartTime: '04/18/2024 11:10:55 EDT',
        flowFilesQueued: 0,
        bytesQueued: 0
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ClusterNodeDetailDialog],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        request: data
                    }
                }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ClusterNodeDetailDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
