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

/**
 * Creates the editor UI view. Model changes are saved by a scripted component service 'implementation'.
 *
 * @argument {object} $scope       The current model
 * @argument {object} $state       The current router state
 * @argument {object} ScriptedComponentFactory       Scripted component service
 * @argument {object} details       Retrieved NiFi scripted component data
 */
var ScriptedComponentController = function ($scope, $state, ScriptedComponentFactory, details) {
    $scope.componentId = '';
    $scope.clientId = '';
    $scope.revisionId = '';
    $scope.error = '';
    $scope.editable = false;
    $scope.editor = {};
    $scope.saveStatus = '';

    /**
     * Converts given map to array.
     *
     * @argument {object} map       The map to be converted
     */
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

    /**
     * Extracts the current script body.
     *
     * @argument {object} details       The component configuration
     */
    $scope.getScriptBody = function (details) {
        return details['properties']['Script Body'] ? details['properties']['Script Body'] : '';
    }

    /**
     * Extracts the chosen script engine.
     *
     * @argument {object} details       The component configuration
     */
    $scope.getScriptEngine = function (details) {
        return details['properties']['Script Engine'] ? details['properties']['Script Engine'] :
            details['descriptors']['Script Engine']['defaultValue'];
    }

    /**
     * Extracts the available script engines.
     *
     * @argument {object} details       The component configuration
     */
    $scope.getScriptEngineOptions = function (details) {
        return $scope.convertToArray(details['descriptors']['Script Engine']['allowableValues']);
    };

    /**
     * Fills the current model with component attributes.
     *
     * @argument {object} details       The component configuration
     */
    $scope.populateScopeWithDetails = function (details) {
        $scope.scriptBody = $scope.getScriptBody(details);
        $scope.scriptEngine = $scope.getScriptEngine(details);
        $scope.scriptEngineOptions = $scope.getScriptEngineOptions(details);
    };

    // fill the scope with scripted component details
    $scope.populateScopeWithDetails(details.data);

    /**
     * Normalizes NiFi script engine name in order to comply with the CodeMirror API contract.
     *
     * @argument {string} scriptEngine       NiFi script engine name
     */
    $scope.mapMode = function (scriptEngine) {
        var scriptEngineNormalized = scriptEngine.toLowerCase();

        // make sure at least some highlighting is on
        return $state.current.data.nameToMime[scriptEngineNormalized] ? $state.current.data.nameToMime[scriptEngineNormalized] : 'application/json';
    }

    /**
     * Sets highlighting mode for the CodeMirror editor based on the selected script engine.
     *
     * @argument {object} editor       CodeMirror editor
     * @argument {string} scriptEngine       NiFi script engine name
     */
    $scope.toggleScriptEngine = function (editor, scriptEngine) {
        editor.setOption('mode', $scope.mapMode(scriptEngine));
    }

    /**
     * Lists globally available highlighting modes of the CodeMirror library.
     *
     */
    $scope.getAvailableModes = function () {
        return CodeMirror.mimeModes;
    }

    /**
     * Indents content of the editor.
     *
     * @argument {object} editor       CodeMirror editor
     */
    $scope.formatEditor = function (editor) {
        // indent every line in the editor
        for (var i = 0; i < editor.lineCount(); i++) {
            editor.indentLine(i);
        }
    }

    /**
     * Initializes the CodeMirror editor instance.
     *
     * @argument {object} editor       CodeMirror editor
     */
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

    /**
     * Creates a scripted component DTO.
     *
     * @argument {string} scriptEngine       NiFi script engine name
     * @argument {string} scriptBody       Script body
     */
    $scope.prepareProperties = function (scriptEngine, scriptBody) {
        return {
            'Script Engine': scriptEngine,
            // default to no value
            'Script Body': scriptBody === '' ? null : scriptBody
        };
    };

    /**
     * Handles errors during save operation.
     *
     * @argument {string} message       Error message
     * @argument {string} detail       Error message details
     */
    $scope.showError = function (message, detail) {
        $scope.error = message;
        console.log('Error received:', detail);
    };


    /**
     * Saves the current model as configuration of the scripted component.
     *
     * @argument {string} scriptEngine       Script engine name
     * @argument {string} scriptBody       Script body
     * @argument {string} componentId       The scripted component ID
     * @argument {string} clientId        The current client ID
     * @argument {number} revisionId        The current scripted component revision ID
     */
    $scope.saveScript = function (scriptEngine, scriptBody, componentId, clientId, revisionId) {
        var properties = $scope.prepareProperties(scriptEngine, scriptBody);

        // save current properties
        ScriptedComponentFactory.setProperties(componentId, revisionId, clientId, properties)
            .then(function (response) {
                var details = response.data;
                $scope.populateScopeWithDetails(details);
                $scope.saveStatus = 'Changes saved successfully';
            })
            .catch(function (response) {
                $scope.showError('Error occurred during save properties', response.statusText);
            });
    };

    /**
     * Populates the view with given component configuration.
     *
     * @argument {object} params       Passed scripted component configuration
     */
    $scope.initController = function (params) {
        $scope.componentId = params.id;
        $scope.clientId = params.clientId;
        $scope.revisionId = params.revision;
        $scope.editable = eval(params.editable);
    };

    // initialize the controller
    $scope.initController($state.params);
};

ScriptedComponentController.$inject = ['$scope', '$state', 'ScriptedComponentFactory', 'details'];

angular.module('standardUI').controller('ScriptedComponentController', ScriptedComponentController);