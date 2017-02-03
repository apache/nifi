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
                'nf.CanvasUtils',
                'nf.Client'],
            function ($, d3, canvasUtils, client) {
                return (nf.Snippet = factory($, d3, canvasUtils, client));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Snippet =
            factory(require('jquery'),
                require('d3'),
                require('nf.CanvasUtils'),
                require('nf.Client')));
    } else {
        nf.Snippet = factory(root.$,
            root.d3,
            root.nf.CanvasUtils,
            root.nf.Client);
    }
}(this, function ($, d3, canvasUtils, client) {
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
         */
        marshal: function (selection) {
            var snippet = {
                parentGroupId: canvasUtils.getGroupId(),
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

                if (canvasUtils.isProcessor(selected)) {
                    snippet.processors[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isFunnel(selected)) {
                    snippet.funnels[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isLabel(selected)) {
                    snippet.labels[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isInputPort(selected)) {
                    snippet.inputPorts[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isOutputPort(selected)) {
                    snippet.outputPorts[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isProcessGroup(selected)) {
                    snippet.processGroups[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isRemoteProcessGroup(selected)) {
                    snippet.remoteProcessGroups[d.id] = client.getRevision(selected.datum());
                } else if (canvasUtils.isConnection(selected)) {
                    snippet.connections[d.id] = client.getRevision(selected.datum());
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
         */
        copy: function (snippetId, origin) {
            var copySnippetRequestEntity = {
                'snippetId': snippetId,
                'originX': origin.x,
                'originY': origin.y
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.processGroups + '/' + encodeURIComponent(canvasUtils.getGroupId()) + '/snippet-instance',
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
                url: config.urls.snippets + '/' + encodeURIComponent(snippetId)
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