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

import { NgClass } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatIconButton } from '@angular/material/button';
import { StatusVariant } from '../../types';

@Component({
    selector: 'status-banner',
    imports: [MatIconButton, NgClass],
    templateUrl: './status-banner.component.html',
    styleUrl: './status-banner.component.scss'
})
export class StatusBanner {
    @Input() variant: StatusVariant = 'critical';
    @Input() allowDismiss = true;

    @Output() dismiss: EventEmitter<void> = new EventEmitter<void>();

    getBannerIcon(variant: StatusVariant): string {
        switch (variant) {
            case 'success':
                return 'fa-check-circle-o';
            case 'info':
            case 'neutral':
                return 'fa-info-circle';
            case 'critical':
            case 'caution':
                return 'fa-warning';
            default:
                return 'fa-warning';
        }
    }

    dismissClicked(): void {
        this.dismiss.next();
    }
}
