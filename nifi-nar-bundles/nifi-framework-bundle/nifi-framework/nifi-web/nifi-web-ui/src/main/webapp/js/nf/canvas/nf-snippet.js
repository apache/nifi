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

/* global nf, d3 */

nf.Snippet = (function () {

    var config = {
        urls: {
            snippets: '../nifi-api/controller/snippets',
            processGroups: '../nifi-api/controller/process-groups'
        }
    };

    return {
        /**
         * Marshals snippet from the specified selection.
         * 
         * @argument {selection} selection      The selection to marshal
         * @argument {boolean} linked   Whether this snippet should be linked to the flow
         */
        marshal: function (selection, linked) {
            var snippet = {
                parentGroupId: nf.Canvas.getGroupId(),
                linked: nf.Common.isDefinedAndNotNull(linked) ? linked : false,
                processorIds: [],
                funnelIds: [],
                inputPortIds: [],
                outputPortIds: [],
                remoteProcessGroupIds: [],
                processGroupIds: [],
                connectionIds: [],
                labelIds: []
            };

            // go through each component and identify its type
            selection.each(function (d) {
                var selected = d3.select(this);

                if (nf.CanvasUtils.isProcessor(selected)) {
                    snippet.processorIds.push(d.component.id);
                } else if (nf.CanvasUtils.isFunnel(selected)) {
                    snippet.funnelIds.push(d.component.id);
                } else if (nf.CanvasUtils.isLabel(selected)) {
                    snippet.labelIds.push(d.component.id);
                } else if (nf.CanvasUtils.isInputPort(selected)) {
                    snippet.inputPortIds.push(d.component.id);
                } else if (nf.CanvasUtils.isOutputPort(selected)) {
                    snippet.outputPortIds.push(d.component.id);
                } else if (nf.CanvasUtils.isProcessGroup(selected)) {
                    snippet.processGroupIds.push(d.component.id);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selected)) {
                    snippet.remoteProcessGroupIds.push(d.component.id);
                } else if (nf.CanvasUtils.isConnection(selected)) {
                    snippet.connectionIds.push(d.component.id);
                }
            });

            return snippet;
        },
        
        /**
         * Creates a snippet.
         * 
         * @argument {object} snippet       The snippet
         */
        create: function (snippet) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'POST',
                url: config.urls.snippets,
                data: $.extend({
                    version: revision.version,
                    clientId: revision.clientId
                }, snippet),
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Copies the snippet to the specified group and origin.
         * 
         * @argument {string} snippetId         The snippet id
         * @argument {string} groupId           The group id
         * @argument {object} origin            The origin
         */
        copy: function (snippetId, groupId, origin) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(groupId) + '/snippet-instance',
                data: {
                    version: revision.version,
                    clientId: revision.clientId,
                    snippetId: snippetId,
                    originX: origin.x,
                    originY: origin.y
                },
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Removes the specified snippet.
         * 
         * @argument {string} snippetId         The snippet id
         */
        remove: function (snippetId) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'DELETE',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId) + '?' + $.param({
                    version: revision.version,
                    clientId: revision.clientId
                })
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Moves the snippet into the specified group.
         * 
         * @argument {string} snippetId         The snippet id
         * @argument {string} newGroupId        The new group id
         */
        move: function (snippetId, newGroupId) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'PUT',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId),
                data: {
                    version: revision.version,
                    clientId: revision.clientId,
                    parentGroupId: newGroupId
                },
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Unlinks the snippet from the actual data flow.
         * 
         * @argument {string} snippetId         The snippet id
         */
        unlink: function (snippetId) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'PUT',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId),
                data: {
                    version: revision.version,
                    clientId: revision.clientId,
                    linked: false
                },
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Links the snippet from the actual data flow.
         * 
         * @argument {string} snippetId         The snippet id
         */
        link: function (snippetId) {
            var revision = nf.Client.getRevision();

            return $.ajax({
                type: 'PUT',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId),
                data: {
                    version: revision.version,
                    clientId: revision.clientId,
                    linked: true
                },
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        }
    };
}());