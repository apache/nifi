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

import NfStorage from 'services/nf-storage.service';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { FdsDialogService } from '@nifi-fds/core';
import { of } from 'rxjs';
import { map, catchError, take, switchMap } from 'rxjs/operators';

var MILLIS_PER_SECOND = 1000;
var headers = new Headers({'Content-Type': 'application/json'});

var config = {
    urls: {
        currentUser: '../nifi-registry-api/access',
        kerberos: '../nifi-registry-api/access/token/kerberos',
        oidc: '../nifi-registry-api/access/oidc/exchange'
    }
};

/**
 * NfRegistryApi constructor.
 *
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param http                  The angular http module.
 * @param fdsDialogService      The FDS dialog service.
 * @constructor
 */
function NfRegistryApi(nfStorage, http, fdsDialogService) {
    this.nfStorage = nfStorage;
    this.http = http;
    this.dialogService = fdsDialogService;
}

NfRegistryApi.prototype = {
    constructor: NfRegistryApi,

    /**
     * Retrieves the snapshot metadata for an existing droplet the registry has stored.
     *
     * @param {string}  dropletUri     The uri of the droplet to request.
     * @returns {*}
     */
    getDropletSnapshotMetadata: function (dropletUri) {
        var self = this;
        var url = '../nifi-registry-api/' + dropletUri;
        url += '/versions';
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Retrieves the specified versioned flow snapshot for an existing droplet the registry has stored.
     *
     * @param {string}  dropletUri      The uri of the droplet to request.
     * @param {number}  versionNumber   The version of the flow to request.
     * @returns {*}
     */
    exportDropletVersionedSnapshot: function (dropletUri, versionNumber) {
        var self = this;
        var url = '../nifi-registry-api/' + dropletUri + '/versions/' + versionNumber + '/export';
        var options = {
            headers: headers,
            observe: 'response',
            responseType: 'text'
        };

        return self.http.get(url, options).pipe(
            map(function (response) {
                // export the VersionedFlowSnapshot by creating a hidden anchor element
                var stringSnapshot = encodeURIComponent(response.body);
                var filename = response.headers.get('Filename');

                var anchorElement = document.createElement('a');
                anchorElement.href = 'data:application/json;charset=utf-8,' + stringSnapshot;
                anchorElement.download = filename;
                anchorElement.style = 'display: none;';

                document.body.appendChild(anchorElement);
                anchorElement.click();
                document.body.removeChild(anchorElement);

                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Uploads a new versioned flow snapshot to the existing droplet the registry has stored.
     *
     * @param {string} dropletUri      The uri of the droplet to request.
     * @param file                     The file to be uploaded.
     * @param {string} comments        The optional comments.
     * @returns {*}
     */
    uploadVersionedFlowSnapshot: function (dropletUri, file, comments) {
        var self = this;
        var url = '../nifi-registry-api/' + dropletUri + '/versions/import';
        var versionHeaders = new HttpHeaders()
            .set('Content-Type', 'application/json')
            .set('Comments', comments);

        return self.http.post(url, file, { 'headers': versionHeaders }).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Uploads a new flow to the existing droplet the registry has stored.
     *
     * @param {string} bucketUri      The uri of the droplet to request.
     * @param file                     The file to be uploaded.
     * @param {string} name            The flow name.
     * @param {string} description     The optional description.
     * @returns {*}
     */
    uploadFlow: function (bucketUri, file, name, description) {
        var self = this;

        var url = '../nifi-registry-api/' + bucketUri + '/flows';
        var flow = { 'name': name, 'description': description };

        // first, create Flow version 0
        return self.http.post(url, flow, headers).pipe(
            take(1),
            switchMap(function (response) {
                var flowUri = response.link.href;
                var importVersionUrl = '../nifi-registry-api/' + flowUri + '/versions/import';

                // then, import file as Flow version 1
                return self.http.post(importVersionUrl, file, headers).pipe(
                    map(function (snapshot) {
                        return snapshot;
                    }),
                    catchError(function (error) {
                        // delete Flow version 0
                        var deleteUri = flowUri + '?versions=0';
                        self.deleteDroplet(deleteUri).subscribe(function (response) {
                            return response;
                        });

                        self.dialogService.openConfirm({
                            title: 'Error',
                            message: error.error,
                            acceptButton: 'Ok',
                            acceptButtonColor: 'fds-warn'
                        });
                        return of(error);
                    })
                );
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Retrieves the given droplet with or without snapshot metadata.
     *
     * @param {string}  bucketId        The id of the bucket to request.
     * @param {string}  dropletType     The type of the droplet to request.
     * @param {string}  dropletId       The id of the droplet to request.
     * @returns {*}
     */
    getDroplet: function (bucketId, dropletType, dropletId) {
        var self = this;
        var url = '../nifi-registry-api/buckets/' + bucketId + '/' + dropletType + '/' + dropletId;
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Flow Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Retrieves all droplets across all buckets (unless the `bucketId` is set then this will
     * retrieve all droplets across that specific bucket). Droplets could include flows, extensions, etc.
     *
     * No snapshot metadata ever returned.
     *
     * @param {string} [bucketId] Defines a bucket id for filtering results.
     * @returns {*}
     */
    getDroplets: function (bucketId) {
        var url = '../nifi-registry-api/items';
        if (bucketId) {
            url += '/' + bucketId;
        }
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Delete an existing droplet the registry has stored.
     *
     * @param {string} dropletUri    The portion of the URI describing the type of the droplet and its identifier
     *
     *  Ex:
     *      'flows/1234'
     *      'extension/5678'
     *
     * @returns {*}
     */
    deleteDroplet: function (dropletUri) {
        var self = this;
        return this.http.delete('../nifi-registry-api/' + dropletUri, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Create a named bucket capable of storing NiFi bucket objects (aka droplets) such as flows and extension bundles.
     *
     * @param {string} name  The name of the bucket.
     * @returns {*}
     */
    createBucket: function (name, allowPublicRead) {
        var self = this;
        return this.http.post('../nifi-registry-api/buckets', {
            'name': name,
            'allowPublicRead': allowPublicRead,
            'revision': {
                'version': 0
            }
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Delete an existing bucket in the registry, along with all the objects it is storing.
     *
     * @param {string} bucketId     The identifier of the bucket to be deleted.
     * @param {int} version         The version from the revision of the bucket
     * @returns {*}
     */
    deleteBucket: function (bucketId, version) {
        var self = this;
        return this.http.delete('../nifi-registry-api/buckets/' + bucketId + '?version=' + version, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Get metadata for an existing bucket in the registry.
     *
     * @param {string} bucketId     The identifier of the bucket to retrieve.
     * @returns {*}
     */
    getBucket: function (bucketId) {
        var self = this;
        var url = '../nifi-registry-api/buckets/' + bucketId;
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Bucket Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Get metadata for all buckets in the registry for which the client is authorized.
     *
     * NOTE: Information about the items stored in each bucket should be obtained by
     * requesting and individual bucket by id.
     *
     * @returns {*}
     */
    getBuckets: function () {
        var self = this;
        var url = '../nifi-registry-api/buckets';
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Buckets Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Updates a bucket.
     *
     * @param {Object} updatedBucket           Object containing The identifier and name of the bucket as well as Whether or not the bucket allows redeploying released bundles.
     * @returns {*}
     */
    updateBucket: function (updatedBucket) {
        return this.http.put('../nifi-registry-api/buckets/' + updatedBucket.identifier, updatedBucket, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Get user by id.
     *
     * @param userId
     * @returns {*}
     */
    getUser: function (userId) {
        var self = this;
        return this.http.get('../nifi-registry-api/tenants/users/' + userId).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'User Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Creates a user.
     *
     * @param {string} identity     The identity of the user.
     * @returns {*}
     */
    addUser: function (identity) {
        var self = this;
        return this.http.post('../nifi-registry-api/tenants/users', {
            identity: identity,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            },
            revision: {
                version: 0
            }
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Updates a user.
     *
     * @param {string} identifier   The identifier of the user.
     * @param {string} identity     The identity of the user.
     * @param {string} revision     The revision of the user.
     * @returns {*}
     */
    updateUser: function (identifier, identity, revision) {
        return this.http.put('../nifi-registry-api/tenants/users/' + identifier, {
            'identifier': identifier,
            'identity': identity,
            'revision': revision
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Gets all users.
     *
     * @returns {*}
     */
    getUsers: function () {
        var self = this;
        return this.http.get('../nifi-registry-api/tenants/users').pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Users Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Delete an existing user from the registry.
     *
     * @param {string} userId     The identifier of the user to be deleted.
     * @param {int} version       The version from the user's revision
     * @returns {*}
     */
    deleteUser: function (userId, version) {
        var self = this;
        return this.http.delete('../nifi-registry-api/tenants/users/' + userId + '?version=' + version, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Gets all user groups.
     *
     * @returns {*}
     */
    getUserGroups: function () {
        var self = this;
        return this.http.get('../nifi-registry-api/tenants/user-groups').pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Groups Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Get user group.
     *
     * @param {string} groupId  The id of the group to retrieve.
     * @returns {*}
     */
    getUserGroup: function (groupId) {
        var self = this;
        return this.http.get('../nifi-registry-api/tenants/user-groups/' + groupId).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Group Not Found',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Delete an existing user group from the registry.
     *
     * @param {string} userGroupId     The identifier of the user group to be deleted.
     * @param {int} version            The version from the group's revision
     * @returns {*}
     */
    deleteUserGroup: function (userGroupId, version) {
        var self = this;
        return this.http.delete('../nifi-registry-api/tenants/user-groups/' + userGroupId + '?version=' + version, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Creates a new group.
     *
     * @param {string}  identifier      The identifier of the user.
     * @param {string}  identity        The identity of the user.
     * @param {array}   users           The array of users to be added to the new group.
     * @returns {*}
     */
    createNewGroup: function (identifier, identity, users) {
        var self = this;
        return this.http.post('../nifi-registry-api/tenants/user-groups', {
            'identifier': identifier,
            'identity': identity,
            'users': users,
            'revision': {
                'version': 0
            }
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Updates a group.
     *
     * @param {string}  identifier   The identifier of the group.
     * @param {string}  identity     The identity of the group.
     * @param {array}   users        The array of users in the new group.
     * @param {string}  revision     The revision of the group.
     * @returns {*}
     */
    updateUserGroup: function (identifier, identity, users, revision) {
        return this.http.put('../nifi-registry-api/tenants/user-groups/' + identifier, {
            'identifier': identifier,
            'identity': identity,
            'users': users,
            'revision': revision
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Get metadata for all policies in the registry for which the client is authorized.
     *
     * @returns {*}
     */
    getPolicies: function () {
        var url = '../nifi-registry-api/policies';
        return this.http.get(url).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Get resource policies by resource identifier.
     *
     * @param {string} action       The name of the resource action (e.g. READ/WRITE/DELETE).
     * @param {string} resource     The name of the resource (e.g. /buckets, /tenants, /policies, /proxy).
     * @param {string} resourceId   The resource identifier.
     * @returns {*}
     */
    getResourcePoliciesById: function (action, resource, resourceId) {
        return this.http.get('../nifi-registry-api/policies/' + action + resource + '/' + resourceId).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Get policy action resource.
     *
     * @param {string} action   The name of the resource action (e.g. READ/WRITE/DELETE).
     * @param {string} resource     The name of the resource (e.g. /buckets, /tenants, /policies, /proxy).
     * @returns {*}
     */
    getPolicyActionResource: function (action, resource) {
        return this.http.get('../nifi-registry-api/policies/' + action + resource).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of(error);
            })
        );
    },

    /**
     * Update policy action resource.
     *
     * @param {string} identifier   The identifier of the group.
     * @param {string} action       The name of the resource action (e.g. READ/WRITE/DELETE).
     * @param {string} resource     The name of the resource (e.g. /buckets, /tenants, /policies, /proxy).
     * @param {string} users        The users with resource privileges.
     * @param {string} userGroups   The user groups with resource privileges.
     * @returns {*}
     */
    putPolicyActionResource: function (identifier, action, resource, users, userGroups, revision) {
        var self = this;
        return this.http.put('../nifi-registry-api/policies/' + identifier, {
            'identifier': identifier,
            'resource': resource,
            'action': action,
            'users': users,
            'userGroups': userGroups,
            'revision': revision
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Creates a policy action resource.
     *
     * @param {string} action       The name of the resource action (e.g. READ/WRITE/DELETE).
     * @param {string} resource     The name of the resource (e.g. /buckets, /tenants, /policies, /proxy).
     * @param {string} users        The users with resource privileges.
     * @param {string} userGroups   The user groups with resource privileges.
     * @returns {*}
     */
    postPolicyActionResource: function (action, resource, users, userGroups) {
        var self = this;
        return this.http.post('../nifi-registry-api/policies', {
            'resource': resource,
            'action': action,
            'users': users,
            'userGroups': userGroups,
            'revision': {
                'version': 0
            }
        }, headers).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: error.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of(error);
            })
        );
    },

    /**
     * Authenticate a user.
     *
     * @param {string} username     The name of the user to authenticate.
     * @param {string} password     The user password.
     * @returns {*}
     */
    postToLogin: function (username, password) {
        var self = this;

        var encodedCredentials = btoa(username + ':' + password);
        var headers = new HttpHeaders({
            'Authorization': 'Basic ' + encodedCredentials
        });

        var options = {
            headers: headers,
            withCredentials: true,
            responseType: 'text'
        };
        return this.http.post('../nifi-registry-api/access/token/login', null, options).pipe(
            map(function (jwt) {
                // get the payload and store the token with the appropriate expiration
                var token = self.nfStorage.getJwtPayload(jwt);
                if (token) {
                    var expiration = parseInt(token['exp'], 10) * MILLIS_PER_SECOND;
                    self.nfStorage.setItem('jwt', jwt, expiration);
                }
                return jwt;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: 'Please contact your System Administrator.',
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of('');
            })
        );
    },

    /**
     * Logout a user.
     *
     * @returns {*}
     */
    deleteToLogout: function (url) {
        var self = this;
        var options = {
            headers: headers,
            withCredentials: true,
            responseType: 'text'
        };

        return this.http.delete(url, options).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: 'Please contact your System Administrator.',
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
                return of('');
            })
        );
    },

    /**
     * Kerberos and OIDC ticket exchange.
     *
     * @returns {*}
     */
    ticketExchange: function () {
        var self = this;
        if (this.nfStorage.hasItem('jwt')) {
            return of(self.nfStorage.getItem('jwt'));
        }
        var jwtHandler = function (jwt) {
            // get the payload and store the token with the appropriate expiration
            var token = self.nfStorage.getJwtPayload(jwt);
            if (token) {
                var expiration = parseInt(token['exp'], 10) * MILLIS_PER_SECOND;
                self.nfStorage.setItem('jwt', jwt, expiration);
            }
            return jwt;
        };
        return this.http.post(config.urls.kerberos, null, {responseType: 'text'}).pipe(
            map(function (jwt) {
                return jwtHandler(jwt);
            }),
            catchError(function (error) {
                return self.http.post(config.urls.oidc, null, {responseType: 'text', withCredentials: 'true'}).pipe(
                    map(function (jwt) {
                        return jwtHandler(jwt);
                    }),
                    catchError(function (error) {
                        return of('');
                    })
                );
            })
        );
    },

    /**
     * Loads the current user and updates the current user locally.
     *
     * @returns xhr
     */
    loadCurrentUser: function () {
        // get the current user
        return this.http.get(config.urls.currentUser).pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                return of({
                    error: error,
                    resourcePermissions: {
                        anyTopLevelResource: {
                            canRead: false,
                            canWrite: false,
                            canDelete: false
                        },
                        buckets: {
                            canRead: false,
                            canWrite: false,
                            canDelete: false
                        },
                        tenants: {
                            canRead: false,
                            canWrite: false,
                            canDelete: false
                        },
                        policies: {
                            canRead: false,
                            canWrite: false,
                            canDelete: false
                        },
                        proxy: {
                            canRead: false,
                            canWrite: false,
                            canDelete: false
                        }
                    }
                });
            })
        );
    },

    /**
     * Get NiFi Registry configuration resource.
     *
     * @returns {*}
     */
    getRegistryConfig: function (action, resource) {
        return this.http.get('../nifi-registry-api/config').pipe(
            map(function (response) {
                return response;
            }),
            catchError(function (error) {
                // If failed, return an empty object.
                return of({});
            })
        );
    }

};

NfRegistryApi.parameters = [
    NfStorage,
    HttpClient,
    FdsDialogService
];

export default NfRegistryApi;
