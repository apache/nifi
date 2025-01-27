/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Input } from '@angular/core';
import { PropertyHintTip } from '../tooltips/property-hint-tip/property-hint-tip.component';
import { NgTemplateOutlet } from '@angular/common';
import { NifiTooltipDirective } from '../../directives/nifi-tooltip.directive';
import { PropertyHintTipInput } from '../../types';

@Component({
    selector: 'property-hint',
    imports: [NgTemplateOutlet, NifiTooltipDirective],
    templateUrl: './property-hint.component.html',
    styleUrl: './property-hint.component.scss'
})
export class PropertyHint {
    @Input() supportsEl: boolean = true;
    @Input() showParameters: boolean = true;
    @Input() supportsParameters: boolean = true;
    @Input() hasParameterContext: boolean = false;

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    getPropertyHintTipData(): PropertyHintTipInput {
        return {
            supportsEl: this.supportsEl,
            showParameters: this.showParameters,
            supportsParameters: this.supportsParameters,
            hasParameterContext: this.hasParameterContext
        };
    }

    protected readonly PropertyHintTip = PropertyHintTip;
}
