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

import { Component, EventEmitter, HostListener, Input, Output, ViewEncapsulation } from '@angular/core';
import { StatusVariant } from '../../types';
import { StatusBanner } from '../status-banner/status-banner.component';

@Component({
    selector: 'banner',
    standalone: true,
    imports: [StatusBanner],
    templateUrl: './banner.component.html',
    styleUrl: './banner.component.scss',
    encapsulation: ViewEncapsulation.None
})
export class Banner {
    @Input() messages: string[] | null = null;
    @Input() variant: StatusVariant = 'critical';
    @Input() allowDismiss = true;
    @Input() allowDismissHotKey?: boolean;
    @Input() panelClass?: string;

    @Output() dismiss: EventEmitter<void> = new EventEmitter<void>();

    dismissClicked(): void {
        this.dismiss.next();
    }

    @HostListener('window:keydown.escape', ['$event'])
    handleKeyDownEscape(event: Event): void {
        if (this.allowDismissHotKey) {
            event.stopPropagation();
            this.dismissClicked();
        }
    }

    get deduplicatedMessages(): string[] {
        if (!this.messages) {
            return [];
        }

        return [...new Set([...this.messages].reverse())];
    }
}
