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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'd3',
                'nf.Storage',
                'nf.CanvasUtils',
                'nf.Client'],
            function ($, d3, nfStorage, nfCanvasUtils, nfClient) {
                return (nf.Snippet = factory($, d3, nfStorage, nfCanvasUtils, nfClient));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Snippet =
            factory(require('jquery'),
                require('d3'),
                require('nf.Storage'),
                require('nf.CanvasUtils'),
                require('nf.Client')));
    } else {
        nf.Snippet = factory(root.$,
            root.d3,
            root.nf.Storage,
            root.nf.CanvasUtils,
            root.nf.Client);
    }
}(this, function ($, d3, nfStorage, nfCanvasUtils, nfClient) {
    'use strict';

    var config = {
        urls: {
            snippets: '../nifi-api/snippets',
            processGroups: '../nifi-api/process-groups'
        }
    };

    return {

        /**
         * Marshals snippet from the specified selection.
         *
         * @argument {selection} selection      The selection to marshal
         * @argument {string} parentGroupId     The parent group id
         */
        marshal: function (selection, parentGroupId) {
            var snippet = {
                parentGroupId: parentGroupId,
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

                if (nfCanvasUtils.isProcessor(selected)) {
                    snippet.processors[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isFunnel(selected)) {
                    snippet.funnels[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isLabel(selected)) {
                    snippet.labels[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isInputPort(selected)) {
                    snippet.inputPorts[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isOutputPort(selected)) {
                    snippet.outputPorts[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isProcessGroup(selected)) {
                    snippet.processGroups[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isRemoteProcessGroup(selected)) {
                    snippet.remoteProcessGroups[d.id] = nfClient.getRevision(selected.datum());
                } else if (nfCanvasUtils.isConnection(selected)) {
                    snippet.connections[d.id] = nfClient.getRevision(selected.datum());
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
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'snippet': snippet
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.snippets,
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
         * @argument {string} destinationGroupId    The destination group id
         */
        copy: function (snippetId, origin, destinationGroupId) {
            var copySnippetRequestEntity = {
                'snippetId': snippetId,
                'originX': origin.x,
                'originY': origin.y,
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(destinationGroupId) + '/snippet-instance',
                data: JSON.stringify(copySnippetRequestEntity),
                dataType: 'json',
                contentType: 'application/json'
            });
        },

        /**
         * Removes the specified snippet.
         *
         * @argument {string} snippetId         The snippet id
         */
        remove: function (snippetId) {
            return $.ajax({
                type: 'DELETE',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId) + '?' + $.param({
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                })
            });
        },

        /**
         * Moves the snippet into the specified group.
         *
         * @argument {string} snippetId         The snippet id
         * @argument {string} newGroupId        The new group id
         */
        move: function (snippetId, newGroupId) {
            var moveSnippetEntity = {
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'snippet': {
                    'id': snippetId,
                    'parentGroupId': newGroupId
                }
            };

            return $.ajax({
                type: 'PUT',
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId),
                data: JSON.stringify(moveSnippetEntity),
                dataType: 'json',
                contentType: 'application/json'
            });
        }
    };
}));