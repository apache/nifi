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

nf.ng.AppConfig = function ($mdThemingProvider, $compileProvider) {
    //Enable production mode, to re-enable debug mode open up a
    //console and call 'angular.reloadWithDebugInfo();'
    $compileProvider.debugInfoEnabled(false);
    //Define app palettes
    $mdThemingProvider.definePalette('basePalette', {
        '50': '728E9B',
        '100': '728E9B',
        '200': '004849', /* link-color */
        '300': '775351', /* value-color */
        '400': '728E9B',
        '500': '728E9B', /* base-color */
        '600': '728E9B',
        '700': '728E9B',
        '800': '728E9B',
        '900': 'rgba(249,250,251,0.97)', /* tint base-color 96% */
        'A100': '728E9B',
        'A200': '728E9B',
        'A400': '728E9B',
        'A700': '728E9B',
        'contrastDefaultColor': 'light',
        'contrastDarkColors': ['A100'],
        'contrastLightColors': undefined
    });
    $mdThemingProvider.definePalette('tintPalette', {
        '50': '728E9B',
        '100': '728E9B',
        '200': 'CCDADB', /* tint link-color 20% */
        '300': '728E9B',
        '400': 'AABBC3', /* tint base-color 40% */
        '500': '728E9B',
        '600': 'C7D2D7', /* tint base-color 60% */
        '700': '728E9B',
        '800': 'E3E8EB', /* tint base-color 80% */
        '900': '728E9B',
        'A100': '728E9B',
        'A200': '728E9B',
        'A400': '728E9B',
        'A700': '728E9B',
        'contrastDefaultColor': 'light',
        'contrastDarkColors': ['A100'],
        'contrastLightColors': undefined
    });
    $mdThemingProvider.definePalette('warnPalette', {
        '50': 'f0ad4e',
        '100': 'f0ad4e',
        '200': 'f0ad4e',
        '300': 'f0ad4e',
        '400': 'f0ad4e',
        '500': '2B5C76', /* warn-color */
        '600': 'f0ad4e',
        '700': 'f0ad4e',
        '800': 'f0ad4e',
        '900': 'f0ad4e',
        'A100': 'f0ad4e',
        'A200': 'f0ad4e',
        'A400': 'f0ad4e',
        'A700': 'f0ad4e',
        'contrastDefaultColor': 'light',
        'contrastDarkColors': ['A100'],
        'contrastLightColors': undefined
    });
    $mdThemingProvider.theme("default").primaryPalette("basePalette", {
        "default": "500",
        "hue-1": "200",
        "hue-2": "300",
        "hue-3": "900"
    }).accentPalette("tintPalette", {
        "default": "200",
        "hue-1": "400",
        "hue-2": "600",
        "hue-3": "800"
    }).warnPalette("warnPalette", {
        "default": "500"
    });
};