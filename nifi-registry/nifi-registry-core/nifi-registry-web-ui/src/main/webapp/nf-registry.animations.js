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

import * as ngAnimate from '@angular/animations';

/**
 * NfAnimations constructor.
 *
 * @constructor
 */
function NfAnimations() {
}

NfAnimations.prototype = {
    constructor: NfAnimations,

    /**
     * Fade animation
     */
    fadeAnimation: ngAnimate.trigger('routeAnimation', [
        ngAnimate.state('*',
            ngAnimate.style({
                opacity: 1
            })),
        ngAnimate.transition(':enter', [
            ngAnimate.style({
                opacity: 0
            }),
            ngAnimate.animate('0.5s ease-in')
        ]),
        ngAnimate.transition(':leave', [
            ngAnimate.animate('0.5s ease-out', ngAnimate.style({
                opacity: 0
            }))
        ])
    ]),

    /**
     * Slide in from the left animation
     */
    slideInLeftAnimation: ngAnimate.trigger('routeAnimation', [
        ngAnimate.state('*',
            ngAnimate.style({
                opacity: 1,
                transform: 'translateX(0)'
            })),
        ngAnimate.transition(':enter', [
            ngAnimate.style({
                opacity: 0,
                transform: 'translateX(-100%)'
            }),
            ngAnimate.animate('0.5s ease-in')
        ]),
        ngAnimate.transition(':leave', [
            ngAnimate.animate('0.5s ease-out', ngAnimate.style({
                opacity: 0,
                transform: 'translateX(100%)'
            }))
        ])
    ]),

    /**
     * Slide in from the top animation
     */
    slideInDownAnimation: ngAnimate.trigger('routeAnimation', [
        ngAnimate.state('*',
            ngAnimate.style({
                opacity: 1,
                transform: 'translateY(0)'
            })),
        ngAnimate.transition(':enter', [
            ngAnimate.style({
                opacity: 0,
                transform: 'translateY(-100%)'
            }),
            ngAnimate.animate('0.5s ease-in')
        ]),
        ngAnimate.transition(':leave', [
            ngAnimate.animate('0.5s ease-out', ngAnimate.style({
                opacity: 0,
                transform: 'translateY(100%)'
            }))
        ])
    ]),

    /**
     * Fly in/out animation
     */
    flyInOutAnimation: ngAnimate.trigger('flyInOut', [
        ngAnimate.state('in',
            ngAnimate.style({transform: 'translateX(0)'})),
        ngAnimate.transition('void => *', [
            ngAnimate.style({transform: 'translateX(100%)'}),
            ngAnimate.animate('0.4s 0.1s ease-in')
        ]),
        ngAnimate.transition('* => void', ngAnimate.animate('0.2s ease-out', ngAnimate.style({transform: 'translateX(-100%)'})))
    ]),

    /**
     * Fly in/out animation
     */
    fadeInOutAnimation: ngAnimate.trigger('fadeInOut', [
        ngAnimate.state('in',
            ngAnimate.style({opacity: 1})),
        ngAnimate.transition('void => *', [
            ngAnimate.style({opacity: 0}),
            ngAnimate.animate('0.5s 0.1s ease-in')
        ]),
        ngAnimate.transition('* => void', ngAnimate.animate('0.5s ease-out', ngAnimate.style({opacity: 0})))
    ])

};

export default new NfAnimations();
