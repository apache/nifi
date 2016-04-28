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
            processGroups: '../nifi-api/process-groups'
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
                processors: [],
                funnels: [],
                inputPorts: [],
                outputPorts: [],
                remoteProcessGroups: [],
                processGroups: [],
                connections: [],
                labels: []
            };

            // go through each component and identify its type
            selection.each(function (d) {
                var selected = d3.select(this);

                if (nf.CanvasUtils.isProcessor(selected)) {
                    snippet.processors.push(d.component.id);
                } else if (nf.CanvasUtils.isFunnel(selected)) {
                    snippet.funnels.push(d.component.id);
                } else if (nf.CanvasUtils.isLabel(selected)) {
                    snippet.labels.push(d.component.id);
                } else if (nf.CanvasUtils.isInputPort(selected)) {
                    snippet.inputPorts.push(d.component.id);
                } else if (nf.CanvasUtils.isOutputPort(selected)) {
                    snippet.outputPorts.push(d.component.id);
                } else if (nf.CanvasUtils.isProcessGroup(selected)) {
                    snippet.processGroups.push(d.component.id);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selected)) {
                    snippet.remoteProcessGroups.push(d.component.id);
                } else if (nf.CanvasUtils.isConnection(selected)) {
                    snippet.connections.push(d.component.id);
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
            var snippetEntity = {
                'revision': nf.Client.getRevision(),
                'snippet': snippet
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets',
                data: JSON.stringify(snippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        },
        
        /**
         * Copies the snippet to the specified group and origin.
         * 
         * @argument {string} snippetId         The snippet id
         * @argument {object} origin            The origin
         */
        copy: function (snippetId, origin) {
            var copySnippetRequestEntity = {
                'revision': nf.Client.getRevision(),
                'snippetId': snippetId,
                'originX': origin.x,
                'originY': origin.y
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippet-instance',
                data: JSON.stringify(copySnippetRequestEntity),
                dataType: 'json',
                contentType: 'application/json'
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
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetId) + '?' + $.param({
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
            var snippetEntity = {
                'revision': nf.Client.getRevision(),
                'snippet': {
                    'id': snippetId,
                    'parentGroupId': newGroupId
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetId),
                data: JSON.stringify(snippetEntity),
                dataType: 'json',
                contentType: 'application/json'
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
            var snippetEntity = {
                'revision': nf.Client.getRevision(),
                'snippet': {
                    'id': snippetId,
                    'linked': false
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetId),
                data: JSON.stringify(snippetEntity),
                dataType: 'json',
                contentType: 'application/json'
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
            var snippetEntity = {
                'revision': nf.Client.getRevision(),
                'snippet': {
                    'id': snippetId,
                    'linked': true
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetId),
                data: JSON.stringify(snippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);
            });
        }
    };
}());