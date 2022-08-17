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
import NfRegistryImportNewFlow from 'components/explorer/grid-list/dialogs/import-new-flow/nf-registry-import-new-flow';

describe('NfRegistryImportNewFlow Component unit tests', function () {
    var nfRegistryApi;
    var data;
    var comp;
    var testFile;

    beforeEach(function () {
        nfRegistryApi = new NfRegistryApi();
        testFile = new File([], 'filename.json');
        data = {
            activeBucket: {},
            buckets: [
                {
                    allowBundleRedeploy: false,
                    allowPublicRead: false,
                    createdTimestamp: 1620168925108,
                    identifier: '123',
                    link: {
                        href: 'buckets/123',
                        params: {}
                    },
                    name: 'Bucket 1',
                    permissions: {canDelete: true, canRead: true, canWrite: true},
                    revision: {version: 0}
                },
                {
                    allowBundleRedeploy: false,
                    allowPublicRead: false,
                    createdTimestamp: 1620168925108,
                    identifier: '456',
                    link: {
                        href: 'buckets/456',
                        params: {}
                    },
                    name: 'Bucket 2',
                    permissions: {canDelete: true, canRead: true, canWrite: false},
                    revision: {version: 0}
                },
            ]
        };

        comp = new NfRegistryImportNewFlow(nfRegistryApi, { openCoaster: function () {} }, { close: function () {} }, data);

        //Spy
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

    it('should assign writable and active buckets on init', function () {
        comp.ngOnInit();
        expect(comp.writableBuckets.length).toBe(1);
        expect(comp.activeBucket).toBe(data.buckets[0].identifier);
    });

    it('should handle file input', function () {
        var jsonFilename = 'filename.json';

        // The function to test
        comp.handleFileInput([testFile]);

        //assertions
        expect(comp.fileToUpload.name).toEqual(jsonFilename);
        expect(comp.fileName).toEqual('filename');
    });
});
