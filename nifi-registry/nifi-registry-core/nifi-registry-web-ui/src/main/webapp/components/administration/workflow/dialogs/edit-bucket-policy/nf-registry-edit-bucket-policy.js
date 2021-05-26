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

import { Component, ViewChild } from '@angular/core';
import NfRegistryService from 'services/nf-registry.service';
import NfRegistryApi from 'services/nf-registry.api';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { ActivatedRoute } from '@angular/router';
import { switchMap } from 'rxjs/operators';
import { forkJoin } from 'rxjs';

/**
 * NfRegistryEditBucketPolicy constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfRegistryService     The nf-registry.service module.
 * @param activatedRoute        The angular route module.
 * @param matDialogRef          The angular material dialog ref.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryEditBucketPolicy(nfRegistryApi, nfRegistryService, activatedRoute, matDialogRef, data) {
    this.userOrGroup = {};
    // Services
    this.nfRegistryService = nfRegistryService;
    this.route = activatedRoute;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    this.data = data;
}

NfRegistryEditBucketPolicy.prototype = {
    constructor: NfRegistryEditBucketPolicy,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.route.params
            .pipe(
                switchMap(function (params) {
                    return forkJoin(
                        self.nfRegistryApi.getUsers(),
                        self.nfRegistryApi.getUserGroups()
                    );
                })
            )
            .subscribe(function (response) {
                var users = response[0];
                var groups = response[1];
                users = users.filter(function (user) {
                    return (self.data.userOrGroup.identity === user.identity);
                });
                if (users.length === 0) {
                    groups = groups.filter(function (group) {
                        return (self.data.userOrGroup.identity === group.identity);
                    });
                    self.userOrGroup = groups[0];
                    self.userOrGroup.type = 'group';
                } else {
                    self.userOrGroup = users[0];
                    self.userOrGroup.type = 'user';
                }
                self.data.userOrGroup.permissions.split(', ').forEach(function (permission) {
                    if (permission === 'read') {
                        self.readCheckbox.checked = true;
                    }
                    if (permission === 'write') {
                        self.writeCheckbox.checked = true;
                    }
                    if (permission === 'delete') {
                        self.deleteCheckbox.checked = true;
                    }
                });
            });
    },

    /**
     * Update a new policy.
     */
    applyPolicy: function () {
        var self = this;
        var action = '';
        var resource = '/buckets';
        var permissions = [];
        if (this.readCheckbox.checked) {
            action = 'read';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'read';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        } else {
            action = 'read';
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'read';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist so we have nothing to do
                } else {
                    // resource exists, let's remove it
                    if (self.userOrGroup.type === 'user') {
                        policy.users = policy.users.filter(function (user) {
                            return (self.userOrGroup.identity !== user.identity);
                        });
                    } else {
                        policy.userGroups = policy.userGroups.filter(function (group) {
                            return (self.userOrGroup.identity !== group.identity);
                        });
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        }
        if (this.writeCheckbox.checked) {
            action = 'write';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'write';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        } else {
            action = 'write';
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'write';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist so we have nothing to do
                } else {
                    // resource exists, let's remove it
                    if (self.userOrGroup.type === 'user') {
                        policy.users = policy.users.filter(function (user) {
                            return (self.userOrGroup.identity !== user.identity);
                        });
                    } else {
                        policy.userGroups = policy.userGroups.filter(function (group) {
                            return (self.userOrGroup.identity !== group.identity);
                        });
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        }
        if (this.deleteCheckbox.checked) {
            action = 'delete';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'delete';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        } else {
            action = 'delete';
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'delete';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist so we have nothing to do
                } else {
                    // resource exists, let's remove it
                    if (self.userOrGroup.type === 'user') {
                        policy.users = policy.users.filter(function (user) {
                            return (self.userOrGroup.identity !== user.identity);
                        });
                    } else {
                        policy.userGroups = policy.userGroups.filter(function (group) {
                            return (self.userOrGroup.identity !== group.identity);
                        });
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                            });
                        }
                    );
                }
            });
        }
        this.dialogRef.close({userOrGroup: self.userOrGroup, permissions: permissions});
    },

    /**
     * Toggle all permission checkboxes.
     *
     * @param $event
     */
    toggleAllPermissions: function ($event) {
        if ($event.checked) {
            this.readCheckbox.checked = true;
            this.writeCheckbox.checked = true;
            this.deleteCheckbox.checked = true;
        } else {
            this.readCheckbox.checked = false;
            this.writeCheckbox.checked = false;
            this.deleteCheckbox.checked = false;
        }
    },

    /**
     * Cancel creation of a new policy and close dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    }
};

NfRegistryEditBucketPolicy.annotations = [
    new Component({
        templateUrl: './nf-registry-edit-bucket-policy.html',
        queries: {
            readCheckbox: new ViewChild('readCheckbox'),
            writeCheckbox: new ViewChild('writeCheckbox'),
            deleteCheckbox: new ViewChild('deleteCheckbox')
        }
    })
];

NfRegistryEditBucketPolicy.parameters = [
    NfRegistryApi,
    NfRegistryService,
    ActivatedRoute,
    MatDialogRef,
    MAT_DIALOG_DATA
];

export default NfRegistryEditBucketPolicy;
