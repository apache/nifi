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

import { AfterViewInit, Component, EventEmitter, inject, Input, Output, ViewChild } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { KeyValuePipe, NgClass, NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatTabsModule } from '@angular/material/tabs';
import { CopyDirective, NiFiCommon, Storage } from '@nifi/shared';
import { MatDrawer, MatDrawerContainer } from '@angular/material/sidenav';
import { Attribute } from '../../../../state/shared';
import { SideBarData } from '../../state/content';
import { MatTooltip } from '@angular/material/tooltip';

@Component({
    selector: 'content-details',
    templateUrl: './content-details.component.html',
    styleUrls: ['./content-details.component.scss'],
    imports: [
        ReactiveFormsModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        NgIf,
        NgForOf,
        MatDatepickerModule,
        MatTabsModule,
        NgTemplateOutlet,
        FormsModule,
        KeyValuePipe,
        CopyDirective,
        MatDrawerContainer,
        MatDrawer,
        NgClass,
        MatTooltip
    ]
})
export class ContentDetails implements AfterViewInit {
    @Input() data!: SideBarData;
    @Output() downloadContent: EventEmitter<string | undefined> = new EventEmitter<string | undefined>();
    @Output() resizeEvent: EventEmitter<boolean> = new EventEmitter<boolean>();

    @ViewChild('drawer') drawer!: MatDrawer;

    private storage: Storage = inject(Storage);
    selectedIndex = 0;
    showDetails = true;

    drawerWidth: number = 400;
    contentWidth: number = 355;
    private toggleBarWidth: number = 50;

    drawerSavedWidth: number = this.drawerWidth;
    contentSavedWidth: number = this.contentWidth;

    private isResizing = false;
    private startX: number = 0;
    private startWidth: number = 0;

    private sidebarContentKey: string = 'content-details-selected-index';
    private sidebarCollapsedKey: string = 'content-details-collapsed';
    private sidebarSizeKey: string = 'content-details-size';

    constructor(private nifiCommon: NiFiCommon) {
        const previousSelectedIndex = this.storage.getItem<number>(this.sidebarContentKey);
        if (previousSelectedIndex != null) {
            this.selectedIndex = previousSelectedIndex;
        }

        const previousSelectedCollapsed = this.storage.getItem<number>(this.sidebarCollapsedKey);
        if (previousSelectedCollapsed != null) {
            this.showDetails = previousSelectedCollapsed == 1;
        }

        const previousSelectedSize = this.storage.getItem<number>(this.sidebarSizeKey);
        if (previousSelectedSize != null) {
            this.drawerSavedWidth = previousSelectedSize;
            this.contentSavedWidth = previousSelectedSize - this.toggleBarWidth;
        }
    }

    formatDurationValue(duration: number): string {
        if (duration === 0) {
            return '< 1 sec';
        }

        return this.nifiCommon.formatDuration(duration);
    }

    tabChanged(selectedTabIndex: number): void {
        this.selectedIndex = selectedTabIndex;
        this.storage.setItem<number>(this.sidebarContentKey, selectedTabIndex);
    }

    toggleDetails() {
        this.showDetails = !this.showDetails;
        this.drawer.toggle(this.showDetails);
        this.storage.setItem<number>(this.sidebarCollapsedKey, this.showDetails ? 1 : 2);
        this.updateWidths();
    }

    ngAfterViewInit(): void {
        this.drawer.toggle(this.showDetails);
        this.updateWidths();
    }

    downloadContentClicked(direction?: string): void {
        this.downloadContent.next(direction);
    }

    attributeValueChanged(attribute: Attribute): boolean {
        return attribute.value != attribute.previousValue;
    }

    startResize(event: MouseEvent): void {
        if (this.showDetails) {
            this.resizeEvent.next(true);
            this.isResizing = true;
            this.startX = event.clientX;
            this.startWidth = this.drawerWidth;

            document.addEventListener('mousemove', this.resizeDrawer);
            document.addEventListener('mouseup', this.stopResize);
            document.addEventListener('mouseleave', this.stopResize);
            window.addEventListener('blur', this.stopResize);
        }
    }

    resizeDrawer = (event: MouseEvent) => {
        if (this.isResizing) {
            const minWidth = 375;
            const delta = this.startX - event.clientX;
            this.drawerWidth = Math.max(minWidth, this.startWidth + delta); // Prevent drawer from shrinking too small
            this.drawerSavedWidth = this.drawerWidth;
            this.contentWidth = this.drawerWidth - this.toggleBarWidth;
            this.contentSavedWidth = this.contentWidth;
        }
    };

    stopResize = (): void => {
        this.isResizing = false;
        document.removeEventListener('mousemove', this.resizeDrawer);
        document.removeEventListener('mouseup', this.stopResize);
        document.removeEventListener('mouseleave', this.stopResize);
        window.removeEventListener('blur', this.stopResize);

        this.resizeEvent.next(false);
        this.storage.setItem<number>(this.sidebarSizeKey, this.drawerSavedWidth);
    };

    updateWidths() {
        if (!this.showDetails) {
            this.drawerWidth = this.toggleBarWidth;
            this.contentWidth = 0;
        } else {
            this.drawerWidth = this.drawerSavedWidth;
            this.contentWidth = this.contentSavedWidth;
        }
    }

    getToolTip() {
        return this.showDetails ? 'Hide Details' : 'Show Details';
    }
}
