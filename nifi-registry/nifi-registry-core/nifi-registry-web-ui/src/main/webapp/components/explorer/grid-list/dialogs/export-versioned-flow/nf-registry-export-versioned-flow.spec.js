/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import NfRegistryApi from 'services/nf-registry.api';
import { of } from 'rxjs';

import NfRegistryExportVersionedFlow from 'components/explorer/grid-list/dialogs/export-versioned-flow/nf-registry-export-versioned-flow';

describe('NfRegistryExportVersionedFlow Component unit tests', function () {
    var nfRegistryApi;
    var comp;

    beforeEach(function () {
        nfRegistryApi = new NfRegistryApi();

        var data = {
            droplet: {
                bucketIdentifier: '123',
                bucketName: 'Bucket 1',
                createdTimestamp: 1620177743648,
                description: '',
                identifier: '555',
                link: {
                    href: 'buckets/123/flows/555',
                    params: {}
                },
                modifiedTimestamp: 1620177743687,
                name: 'Test Flow',
                permissions: {canDelete: true, canRead: true, canWrite: true},
                revision: {version: 0},
                snapshotMetadata: [
                    {
                        author: 'anonymous',
                        bucketIdentifier: '123',
                        comments: 'Test comments',
                        flowIdentifier: '555',
                        link: {
                            href: 'buckets/123/flows/555/versions/1',
                            params: {}
                        }
                    },
                    {
                        author: 'anonymous',
                        bucketIdentifier: '123',
                        comments: 'Test comments',
                        flowIdentifier: '999',
                        link: {
                            href: 'buckets/123/flows/999/versions/2',
                            params: {}
                        }
                    }
                ],
                type: 'Flow',
                versionCount: 2
            }
        };

        comp = new NfRegistryExportVersionedFlow(nfRegistryApi, { openCoaster: function () {} }, { close: function () {} }, data);

        var response = {
            body: {
                flowContents: {
                    componentType: 'PROCESS_GROUP',
                    connections: [],
                    controllerServices: [],
                    funnels: [],
                    identifier: '555',
                    inputPorts: [],
                    labels: [],
                    name: 'Test snapshot',
                    outputPorts: [],
                    processGroups: []
                },
                snapshotMetadata: {
                    author: 'anonymous',
                    bucketIdentifier: '123',
                    comments: 'Test comments',
                    flowIdentifier: '555',
                    link: {
                        href: 'buckets/123/flows/555/versions/2',
                        params: {}
                    },
                    version: 2
                }
            },
            headers: {
                headers: [
                    {'filename': ['Test-flow-version-1']}
                ]
            },
            ok: true,
            status: 200,
            statusText: 'OK',
            type: 4,
            url: 'testUrl'
        };

        // Spy
        spyOn(nfRegistryApi, 'exportDropletVersionedSnapshot').and.callFake(function () {
        }).and.returnValue(of(response));
        spyOn(comp.dialogRef, 'close');
    });

    it('should create component', function () {
        expect(comp).toBeDefined();
    });

    it('should export a versioned flow snapshot and close the dialog', function () {
        spyOn(comp, 'exportVersion').and.callThrough();

        // The function to test
        comp.exportVersion();

        //assertions
        expect(comp).toBeDefined();
        expect(comp.exportVersion).toHaveBeenCalled();
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });

    it('should cancel the export of a flow snapshot', function () {
        // the function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });
});
