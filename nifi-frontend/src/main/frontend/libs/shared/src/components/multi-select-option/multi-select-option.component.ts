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

import { ChangeDetectionStrategy, Component, HostBinding, Input, ViewEncapsulation } from '@angular/core';
import { MatOption } from '@angular/material/select';
import { MatPseudoCheckbox, MatRipple } from '@angular/material/core';

/**
 * Extension of MatOption that shows a trailing checkmark when selected and supports a
 * "virtual" selection flag so mat-select can track selections for rows rendered outside
 * the viewport when used with CDK virtual scrolling.
 */
@Component({
    selector: 'multi-select-option',
    imports: [MatPseudoCheckbox, MatRipple],
    templateUrl: './multi-select-option.component.html',
    styleUrl: './multi-select-option.component.scss',
    encapsulation: ViewEncapsulation.None,
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{ provide: MatOption, useExisting: MultiSelectOption }],
    host: { class: 'multi-select-option', role: 'option' }
})
export class MultiSelectOption extends MatOption {
    /**
     * Virtual selection state - for virtual scrolling when mat-select can't track selection
     */
    @Input() virtuallySelected = false;

    /**
     * Host binding to apply selection styling when virtually selected
     */
    @HostBinding('class.mdc-list-item--selected')
    get isVisuallySelected(): boolean {
        return this.selected || this.virtuallySelected;
    }

    /**
     * Determines if checkmark should be shown
     */
    get shouldShowCheckmark(): boolean {
        return this.selected || this.virtuallySelected;
    }
}
