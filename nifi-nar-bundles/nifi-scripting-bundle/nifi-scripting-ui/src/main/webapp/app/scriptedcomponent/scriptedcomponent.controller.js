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
'use strict';

var ScriptedComponentController = function ($scope, $state, ScriptedComponentFactory, details) {
    $scope.processorId = '';
    $scope.clientId = '';
    $scope.revisionId = '';
    $scope.error = '';
    $scope.editable = false;
    $scope.editor = {};
    $scope.saveStatus = '';

    $scope.convertToArray = function (map) {
        var labelValueArray = [];

        angular.forEach(map, function (value, key) {
            labelValueArray.push({
                'label': value,
                'value': key
            });
        });
        return labelValueArray;
    };

    $scope.getScriptBody = function (details) {
        return details['properties']['Script Body'] ? details['properties']['Script Body'] : '';
    }

    $scope.getScriptEngine = function (details) {
        return details['properties']['Script Engine'] ? details['properties']['Script Engine'] :
            details['descriptors']['Script Engine']['defaultValue'];
    }

    $scope.getScriptEngineOptions = function (details) {
        return $scope.convertToArray(details['descriptors']['Script Engine']['allowableValues']);
    };

    $scope.populateScopeWithDetails = function (details) {
        $scope.scriptBody = $scope.getScriptBody(details);
        $scope.scriptEngine = $scope.getScriptEngine(details);
        $scope.scriptEngineOptions = $scope.getScriptEngineOptions(details);
    };

    // populete the scope with processor details
    $scope.populateScopeWithDetails(details.data);

    $scope.mapMode = function (scriptEngine) {
        var scriptEngineNormalized = scriptEngine.toLowerCase();

        // make sure at least some highlighting is on
        return $state.current.data.nameToMime[scriptEngineNormalized] ? $state.current.data.nameToMime[scriptEngineNormalized] : 'application/json';
    }

    $scope.toggleScriptEngine = function (editor, scriptEngine) {
        editor.setOption('mode', $scope.mapMode(scriptEngine));
    }

    $scope.getAvailableModes = function () {
        return CodeMirror.mimeModes;
    }

    $scope.formatEditor = function (editor) {
        // indent every line in the editor
        for (var i = 0; i < editor.lineCount(); i++) {
            editor.indentLine(i);
        }
    }

    $scope.initEditor = function (editor) {
        $scope.editor = editor;

        // register Shift+F as an auto-indent combination
        editor.setOption('extraKeys', {
            'Shift-F': $scope.formatEditor
        });

        editor.setOption('readOnly', !$scope.editable);

        // set up the editor mode
        $scope.toggleScriptEngine(editor, $scope.scriptEngine);
    };

    // initial editor configuration
    $scope.editorProperties = {
        lineNumbers: true,
        autofocus: true,
        value: $scope.scriptBody,
        onLoad: $scope.initEditor
    };

    $scope.prepareProperties = function (scriptEngine, scriptBody) {
        return {
            'Script Engine': scriptEngine,
            // default to no value
            'Script Body': scriptBody === '' ? null : scriptBody
        };
    };

    $scope.showError = function (message, detail) {
        $scope.error = message;
        console.log('Error received:', detail);
    };

    $scope.saveScript = function (scriptEngine, scriptBody, processorId, clientId, revisionId) {
        var properties = $scope.prepareProperties(scriptEngine, scriptBody);

        // save current properties
        ScriptedComponentFactory.setProperties(processorId, revisionId, clientId, properties)
            .then(function (response) {
                var details = response.data;
                $scope.populateScopeWithDetails(details);
                $scope.saveStatus = 'Changes saved successfully';
            })
            .catch(function (response) {
                $scope.showError('Error occurred during save properties', response.statusText);
            });
    };

    $scope.initController = function (params) {
        $scope.processorId = params.id;
        $scope.clientId = params.clientId;
        $scope.revisionId = params.revision;
        $scope.editable = eval(params.editable);
    };

    // initialize the controller
    $scope.initController($state.params);
};

ScriptedComponentController.$inject = ['$scope', '$state', 'ScriptedComponentFactory', 'details'];

angular.module('standardUI').controller('ScriptedComponentController', ScriptedComponentController);