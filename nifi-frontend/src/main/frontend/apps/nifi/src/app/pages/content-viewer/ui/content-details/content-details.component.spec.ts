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

import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ContentDetails } from './content-details.component';

describe('ContentDetails', () => {
    let component: ContentDetails;
    let fixture: ComponentFixture<ContentDetails>;

    const flowFile = {
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
    };

    const provEvent = {
        alternateIdentifierUri: '',
        clusterNodeAddress: '',
        eventDuration: '',
        inputContentClaimContainer: '',
        inputContentClaimFileSize: '',
        inputContentClaimFileSizeBytes: 0,
        inputContentClaimIdentifier: '',
        inputContentClaimOffset: 0,
        inputContentClaimSection: '',
        relationship: '',
        sourceSystemFlowFileId: '',
        id: '153',
        eventId: 153,
        eventTime: '04/17/2025 19:15:23.677 UTC',
        lineageDuration: 175602541,
        eventType: 'DOWNLOAD',
        flowFileUuid: 'eaa6126a-2ab6-4e9d-b22e-9263f9b5ca16',
        fileSize: '10 KB',
        fileSizeBytes: 10240,
        clusterNodeId: 'undefined',
        groupId: '38e7d6f2-0195-1000-e73b-12aa5e7be55a',
        componentId: '38e7d6f2-0195-1000-e73b-12aa5e7be55a',
        componentType: 'NiFi Flow',
        componentName: 'NiFi Flow',
        parentUuids: [],
        childUuids: [],
        attributes: [
            {
                name: 'path',
                value: './',
                previousValue: './'
            },
            {
                name: 'filename',
                value: '93a06a31-6b50-4d04-9b45-6a2a4a5e2dd1',
                previousValue: '93a06a31-6b50-4d04-9b45-6a2a4a5e2dd1'
            },
            {
                name: 'uuid',
                value: 'fc165889-3493-404c-9895-62d49a06801b',
                previousValue: 'fc165889-3493-404c-9895-62d49a06801b'
            }
        ],
        transitUri:
            'https://localhost:4200/nifi-api/flowfile-queues/43ccc987-0196-1000-417e-aa3430852b7e/flowfiles/eaa6126a-2ab6-4e9d-b22e-9263f9b5ca16/content',
        details: '',
        contentEqual: false,
        inputContentAvailable: false,
        outputContentAvailable: true,
        outputContentClaimSection: '1',
        outputContentClaimContainer: 'default',
        outputContentClaimIdentifier: '1744741721138-1',
        outputContentClaimOffset: '0',
        outputContentClaimFileSize: '10 KB',
        outputContentClaimFileSizeBytes: 10240,
        replayAvailable: false,
        replayExplanation:
            'Cannot replay data from Provenance Event because the Source FlowFile Queue with ID No Value no longer exists',
        sourceConnectionIdentifier: 'No Value'
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ContentDetails, NoopAnimationsModule]
        });
        fixture = TestBed.createComponent(ContentDetails);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        component.data = {
            flowFile: flowFile,
            provEvent: undefined
        };
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });

    it('should create', () => {
        component.data = {
            flowFile: undefined,
            provEvent: provEvent
        };
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });
});
