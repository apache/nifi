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

var TransformJsonController = function ($scope, $state, $q, $mdDialog, $timeout, TransformJsonService, ProcessorService, details) {

    $scope.processorId = '';
    $scope.clientId = '';
    $scope.revisionId = '';
    $scope.editable = false;
    $scope.disconnectedNodeAcknowledged = false;
    $scope.specEditor = {};
    $scope.inputEditor = {};
    $scope.jsonInput = '';
    $scope.jsonOutput = '';
    $scope.sortOutput = false;
    $scope.validObj = {};
    $scope.error = '';
    $scope.disableCSS = '';
    $scope.saveStatus = '';
    $scope.specUpdated = false;
    $scope.variables = {};
    $scope.joltVariables = {};

    $scope.convertToArray= function (map){
        var labelValueArray = [];
        angular.forEach(map, function(value, key){
            labelValueArray.push({'label' :  value, 'value' : key});
        });
        return labelValueArray;
    };

    $scope.getJsonSpec = function(details){
        if(details['properties']['jolt-spec'] != null && details['properties']['jolt-spec'] != "") {
            return details['properties']['jolt-spec'];
        }
        else return '';
    };

    $scope.getTransform = function(details){
        return details['properties']['jolt-transform'] ? details['properties']['jolt-transform'] :
            details['descriptors']['jolt-transform']['defaultValue'] ;
    };

    $scope.getCustomClass = function(details){
        if(details['properties']['jolt-custom-class'] != null && details['properties']['jolt-custom-class'] != "") {
            return details['properties']['jolt-custom-class'];
        }
        else return '';
    };

    $scope.getCustomModules = function(details){
        if(details['properties']['jolt-custom-modules'] != null && details['properties']['jolt-custom-modules'] != "") {
            return details['properties']['jolt-custom-modules'];
        }
        else return '';
    };

    $scope.getTransformOptions = function(details){
        return $scope.convertToArray(details['descriptors']['jolt-transform']['allowableValues']);
    };

    $scope.populateScopeWithDetails = function(details){
        $scope.jsonSpec = $scope.getJsonSpec(details);
        $scope.transform = $scope.getTransform(details);
        $scope.transformOptions = $scope.getTransformOptions(details);
        $scope.customClass = $scope.getCustomClass(details);
        $scope.modules = $scope.getCustomModules(details);
    };

    $scope.populateScopeWithDetails(details.data);

    $scope.clearValidation = function(){
        $scope.validObj = {};
    };

    $scope.clearError = function(){
        $scope.error = '';
    };

    $scope.clearSave = function(){
        if($scope.saveStatus != ''){
            $scope.saveStatus = '';
        }
    };

    $scope.clearMessages = function(){
        $scope.clearSave();
        $scope.clearError();
        $scope.clearValidation();
    };

    $scope.showError = function(message,detail){
        $scope.error = message;
        console.log('Error received:', detail);
    };

    $scope.initEditors = function(_editor) {

        _editor.setOption('extraKeys',{

            'Shift-F': function(cm){
                var jsonValue = js_beautify(cm.getDoc().getValue(), {
                    'indent_size': 1,
                    'indent_char': '\t'
                });
                cm.getDoc().setValue(jsonValue)
            }

        });

    };

    $scope.initSpecEditor = function(_editor){
        $scope.initEditors(_editor);
        $scope.specEditor = _editor;
        _editor.on('update',function(cm){
            if($scope.transform == 'jolt-transform-sort'){
                $scope.toggleEditorErrors(_editor,'hide');
            }
        });

        _editor.on('change',function(cm,changeObj){

            if(!($scope.transform == 'jolt-transform-sort' && changeObj.text.toString() == "")){
                $scope.clearMessages();
                if(changeObj.text.toString() != changeObj.removed.toString()){
                    $scope.specUpdated = true;
                }
            }
        });

    };

    $scope.initInputEditor = function(_editor){
        $scope.initEditors(_editor);
        $scope.inputEditor = _editor;

        _editor.on('change',function(cm,changeObj){
            $scope.clearMessages();
        });

        _editor.clearGutter('CodeMirror-lint-markers');
    };

    $scope.editorProperties = {
        lineNumbers: true,
        gutters: ['CodeMirror-lint-markers'],
        mode: 'application/json',
        lint: true,
        value: $scope.jsonSpec,
        onLoad: $scope.initSpecEditor
    };

    $scope.inputProperties = {
        lineNumbers: true,
        gutters: ['CodeMirror-lint-markers'],
        mode: 'application/json',
        lint: true,
        value: "",
        onLoad: $scope.initInputEditor
    };

    $scope.outputProperties = {
        lineNumbers: true,
        gutters: ['CodeMirror-lint-markers'],
        mode: 'application/json',
        lint: false,
        readOnly: true
    };

    $scope.formatEditor = function(editor){

        var jsonValue = js_beautify(editor.getDoc().getValue(), {
            'indent_size': 1,
            'indent_char': '\t'
        });
        editor.getDoc().setValue(jsonValue);

    };

    $scope.hasJsonErrors = function(input, transform){
        try{
            jsonlint.parse(input);
        }catch(e){
            return true;
        }
        return false;
    };

    $scope.toggleEditorErrors = function(editor,toggle){
        var display = editor.display.wrapper;
        var errors = display.getElementsByClassName("CodeMirror-lint-marker-error");

        if(toggle == 'hide'){

            angular.forEach(errors,function(error){
                var element = angular.element(error);
                element.addClass('hide');
            });

            var markErrors = display.getElementsByClassName("CodeMirror-lint-mark-error");
            angular.forEach(markErrors,function(error){
                var element = angular.element(error);
                element.addClass('CodeMirror-lint-mark-error-hide');
                element.removeClass('CodeMirror-lint-mark-error');
            });

        }else{

            angular.forEach(errors,function(error){
                var element = angular.element(error);
                element.removeClass('hide');
            });

            var markErrors = display.getElementsByClassName("CodeMirror-lint-mark-error-hide");
            angular.forEach(markErrors,function(error){
                var element = angular.element(error);
                element.addClass('CodeMirror-lint-mark-error');
                element.removeClass('CodeMirror-lint-mark-error-hide');
            });

        }

    };

    $scope.toggleEditor = function(editor,transform,specUpdated){

        if(transform == 'jolt-transform-sort'){
            editor.setOption("readOnly","nocursor");
            $scope.disableCSS = "trans";
            $scope.toggleEditorErrors(editor,'hide');
        }
        else{
            editor.setOption("readOnly",false);
            $scope.disableCSS = "";
            $scope.toggleEditorErrors(editor,'show');
        }

        $scope.specUpdated = specUpdated;

        $scope.clearMessages();

    }

    $scope.getJoltSpec = function(transform,jsonSpec,jsonInput){

        return  {
            "transform": transform,
            "specification" : jsonSpec,
            "input" : jsonInput,
            "customClass" : $scope.customClass,
            "modules": $scope.modules,
            "expressionLanguageAttributes":$scope.joltVariables
        };
    };

    $scope.getProperties = function(transform,jsonSpec){

        return {
            "jolt-transform" : transform != "" ? transform : null,
            "jolt-spec": jsonSpec != "" ? jsonSpec : null
        };

    };

    $scope.getSpec = function(transform,jsonSpec){
        if(transform != 'jolt-transform-sort'){
            return jsonSpec;
        }else{
            return null;
        }
    };

    $scope.validateJson = function(jsonInput,jsonSpec,transform){

        var deferred = $q.defer();

        $scope.clearError();

        if( transform == 'jolt-transform-sort' ||!$scope.hasJsonErrors(jsonSpec,transform) ){
            var joltSpec = $scope.getJoltSpec(transform,jsonSpec,jsonInput);

            TransformJsonService.validate(joltSpec).then(function(response){
                $scope.validObj = response.data;
                deferred.resolve($scope.validObj);
            }).catch(function(response) {
                $scope.showError("Error occurred during validation",response.statusText)
                deferred.reject($scope.error);
            });

        }else{
            $scope.validObj = {"valid":false,"message":"JSON Spec provided is not valid JSON format"};
            deferred.resolve($scope.validObj);
        }

        return deferred.promise;

    };

    $scope.transformJson = function(jsonInput,jsonSpec,transform){


        if( !$scope.hasJsonErrors(jsonInput,transform) ){

            $scope.validateJson(jsonInput,jsonSpec,transform).then(function(response){

                var validObj = response;

                if(validObj.valid == true){

                    var joltSpec = $scope.getJoltSpec(transform,jsonSpec,jsonInput);

                    TransformJsonService.execute(joltSpec).then(function(response){

                            $scope.jsonOutput = js_beautify(response.data, {
                                'indent_size': 1,
                                'indent_char': '\t'
                            });

                        })
                        .catch(function(response) {
                            $scope.showError("Error occurred during transformation",response.statusText)
                        });

                }

            });

        }else{
            $scope.validObj = {"valid":false,"message":"JSON Input provided is not valid JSON format"};
        }
    };

    $scope.saveSpec = function(jsonInput,jsonSpec,transform,processorId,clientId,disconnectedNodeAcknowledged,revisionId){

        $scope.clearError();

        var properties = $scope.getProperties(transform,jsonSpec);

        ProcessorService.setProperties(processorId,revisionId,clientId,disconnectedNodeAcknowledged,properties)
            .then(function(response) {
                var details = response.data;
                $scope.populateScopeWithDetails(details);
                $scope.saveStatus = "Changes saved successfully";
                $scope.specUpdated = false;
            })
            .catch(function(response) {
                $scope.showError("Error occurred during save properties",response.statusText);
            });

    };

    $scope.addVariable = function(variables,key,value){
        if(key != '' && value != ''){
            variables[key] = value;
        }

        $timeout(function() {
            var scroller = document.getElementById("variableList");
            scroller.scrollTop = scroller.scrollHeight;
        }, 0, false);
    }

    $scope.deleteVariable = function(variables,key){
        delete variables[key];
    }

    $scope.saveVariables = function(variables){
        angular.copy($scope.variables,$scope.joltVariables);
        $scope.cancelDialog();
    }

    $scope.cancelDialog = function(){
        $mdDialog.cancel();
    }

    $scope.showVariableDialog = function(ev) {
        angular.copy($scope.joltVariables, $scope.variables);
        $mdDialog.show({
            locals: {parent: $scope},
            controller: angular.noop,
            controllerAs: 'dialogCtl',
            bindToController: true,
            templateUrl: 'app/transformjson/variable-dialog-template.html',
            targetEvent: ev,
            clickOutsideToClose: false
        });

    };

    $scope.initController = function(params){
        $scope.processorId = params.id;
        $scope.clientId = params.clientId;
        $scope.revisionId = params.revision;
        $scope.disconnectedNodeAcknowledged = eval(params.disconnectedNodeAcknowledged);
        $scope.editable = eval(params.editable);

        var jsonSpec = $scope.getSpec($scope.transform,$scope.jsonSpec);
        if(jsonSpec != null && jsonSpec != ""){
            setTimeout(function(){
                $scope.$apply(function(){
                    $scope.validateJson($scope.jsonInput,jsonSpec,$scope.transform);
                });
            });
        }
    };

    $scope.$watch("specEditor", function (newValue, oldValue ) {
        $scope.toggleEditor(newValue,$scope.transform,false);
    });

    $scope.initController($state.params);

};

TransformJsonController.$inject = ['$scope', '$state', '$q','$mdDialog','$timeout','TransformJsonService', 'ProcessorService','details'];
angular.module('standardUI').controller('TransformJsonController', TransformJsonController);
