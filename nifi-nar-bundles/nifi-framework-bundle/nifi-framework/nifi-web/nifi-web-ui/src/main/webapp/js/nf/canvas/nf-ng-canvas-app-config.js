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

nf.ng.Canvas.AppConfig = (function () {

    function AppConfig($mdThemingProvider, $compileProvider) {
        //Enable production mode, to re-enable debug mode open up a
        //console and call 'angular.reloadWithDebugInfo();'
        $compileProvider.debugInfoEnabled(false);
        //Define app palettes
        var basePaletteMap = $mdThemingProvider.extendPalette("grey", {
            "contrastDefaultColor": "light",
            "contrastDarkColors": ["100"], //hues which contrast should be "dark" by default
            "contrastLightColors": ["600"], //hues which contrast should be "light" by default
            "500": "728E9B"
        });
        var accentPaletteMap = $mdThemingProvider.extendPalette("grey", {
            "contrastDefaultColor": "dark",
            "contrastDarkColors": ["100"],
            "contrastLightColors": ["600"],
            "500": "BA554A"
        });
        $mdThemingProvider.definePalette("basePalette", basePaletteMap);
        $mdThemingProvider.definePalette("accentPalette", accentPaletteMap);
        $mdThemingProvider.theme("default").primaryPalette("basePalette", {
            "default": "500",
            "hue-1": "50", // use for the <code>md-hue-1</code> class
            "hue-2": "300", // use for the <code>md-hue-2</code> class
            "hue-3": "600" // use for the <code>md-hue-3</code> class
        }).accentPalette("accentPalette", {
            "default": "500",
            "hue-1": "50",
            "hue-2": "300",
            "hue-3": "600"
        });
    }

    AppConfig.$inject=['$mdThemingProvider', '$compileProvider'];

    return AppConfig;
}());