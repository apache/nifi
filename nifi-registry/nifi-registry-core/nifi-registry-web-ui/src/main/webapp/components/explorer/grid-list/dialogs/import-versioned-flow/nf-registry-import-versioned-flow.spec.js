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
import NfRegistryImportVersionedFlow from 'components/explorer/grid-list/dialogs/import-versioned-flow/nf-registry-import-versioned-flow';

describe('NfRegistryImportVersionedFlow Component unit tests', function () {
    var nfRegistryApi;
    var data;
    var comp;

    beforeEach(function () {
        nfRegistryApi = new NfRegistryApi();

        data = {
            droplet: {
                bucketIdentifier: '123',
                bucketName: 'Bucket 2',
                createdTimestamp: 1620177743648,
                description: '',
                identifier: '555',
                link: {
                    href: 'buckets/123/flows/555',
                    params: {}
                },
                modifiedTimestamp: 1620177743687,
                name: 'Test Flow 2',
                permissions: {canDelete: true, canRead: true, canWrite: true},
                revision: {version: 0},
                snapshotMetadata: [
                    {
                        author: 'anonymous',
                        bucketIdentifier: '123',
                        comments: 'Test comments',
                        flowIdentifier: '555',
                        link: {}
                    }
                ],
                type: 'Flow',
                versionCount: 1
            }
        };

        comp = new NfRegistryImportVersionedFlow(nfRegistryApi, { openCoaster: function () {} }, { close: function () {} }, data);

        // Spy
        spyOn(comp.dialogRef, 'close');
    });

    it('should create component', function () {
        expect(comp).toBeDefined();
    });

    it('should cancel the import of a new version', function () {
        // the function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });

    it('should handle file input', function () {
        var jsonFilename = 'filename.json';
        var testFile = new File([], jsonFilename);

        // The function to test
        comp.handleFileInput([testFile]);

        //assertions
        expect(comp.fileToUpload.name).toEqual(jsonFilename);
        expect(comp.fileName).toEqual('filename');
    });
});
