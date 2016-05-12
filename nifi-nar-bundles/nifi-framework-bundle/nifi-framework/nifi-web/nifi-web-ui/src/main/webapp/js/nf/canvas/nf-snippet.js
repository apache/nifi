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
                processors: {},
                funnels: {},
                inputPorts: {},
                outputPorts: {},
                remoteProcessGroups: {},
                processGroups: {},
                connections: {},
                labels: {}
            };

            // go through each component and identify its type
            selection.each(function (d) {
                var selected = d3.select(this);

                if (nf.CanvasUtils.isProcessor(selected)) {
                    snippet.processors[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isFunnel(selected)) {
                    snippet.funnels[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isLabel(selected)) {
                    snippet.labels[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isInputPort(selected)) {
                    snippet.inputPorts[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isOutputPort(selected)) {
                    snippet.outputPorts[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isProcessGroup(selected)) {
                    snippet.processGroups[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selected)) {
                    snippet.remoteProcessGroups[d.id] = nf.Client.getRevision(selected.datum());
                } else if (nf.CanvasUtils.isConnection(selected)) {
                    snippet.connections[d.id] = nf.Client.getRevision(selected.datum());
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
                'snippet': snippet
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets',
                data: JSON.stringify(snippetEntity),
                dataType: 'json',
                contentType: 'application/json'
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
            });
        },
        
        /**
         * Removes the specified snippet.
         * 
         * @argument {string} snippetEntity         The snippet entity
         */
        remove: function (snippetEntity) {
            var revision = nf.Client.getRevision(snippetEntity);

            return $.ajax({
                type: 'DELETE',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetEntity.id) + '?' + $.param({
                    version: revision.version,
                    clientId: revision.clientId
                })
            });
        },
        
        /**
         * Moves the snippet into the specified group.
         * 
         * @argument {object} snippetEntity         The snippet entity
         * @argument {string} newGroupId        The new group id
         */
        move: function (snippetEntity, newGroupId) {
            var moveSnippetEntity = {
                'revision': nf.Client.getRevision(snippetEntity),
                'snippet': {
                    'id': snippetEntity.id,
                    'parentGroupId': newGroupId
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetEntity.id),
                data: JSON.stringify(moveSnippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            });
        },
        
        /**
         * Unlinks the snippet from the actual data flow.
         * 
         * @argument {object} snippetEntity       The snippet enmtity
         */
        unlink: function (snippetEntity) {
            var unlinkSnippetEntity = {
                'revision': nf.Client.getRevision(snippetEntity),
                'snippet': {
                    'id': snippetEntity.id,
                    'linked': false
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetEntity.id),
                data: JSON.stringify(unlinkSnippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            });
        },
        
        /**
         * Links the snippet from the actual data flow.
         * 
         * @argument {object} snippetEntity         The snippet entity
         */
        link: function (snippetEntity) {
            var linkSnippetEntity = {
                'revision': nf.Client.getRevision(snippetEntity),
                'snippet': {
                    'id': snippetEntity.id,
                    'linked': true
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.processGroups + '/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/snippets/' + encodeURIComponent(snippetEntity.id),
                data: JSON.stringify(linkSnippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            });
        }
    };
}());