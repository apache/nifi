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

import {
    AfterViewInit,
    ChangeDetectorRef,
    Component,
    DestroyRef,
    forwardRef,
    inject,
    Input,
    QueryList,
    ViewChildren
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { Observable, take } from 'rxjs';
import {
    MatCell,
    MatCellDef,
    MatColumnDef,
    MatHeaderCell,
    MatHeaderCellDef,
    MatHeaderRow,
    MatHeaderRowDef,
    MatRow,
    MatRowDef,
    MatTable,
    MatTableDataSource
} from '@angular/material/table';
import {
    CdkConnectedOverlay,
    CdkOverlayOrigin,
    ConnectionPositionPair,
    OriginConnectionPosition,
    OverlayConnectionPosition
} from '@angular/cdk/overlay';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { TextEditor } from './editors/text-editor/text-editor.component';
import { NifiTooltipDirective } from '../../directives/nifi-tooltip.directive';
import { NiFiCommon } from '../../services/nifi-common.service';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { MapTableEntry, MapTableItem } from '../../types';

@Component({
    selector: 'map-table',
    imports: [
        CommonModule,
        CdkConnectedOverlay,
        CdkOverlayOrigin,
        MatCell,
        MatCellDef,
        MatColumnDef,
        MatHeaderCell,
        MatIconButton,
        MatTable,
        MatHeaderCellDef,
        NifiTooltipDirective,
        MatMenuTrigger,
        MatMenu,
        MatMenuItem,
        MatRow,
        MatHeaderRow,
        MatRowDef,
        MatHeaderRowDef,
        TextEditor
    ],
    templateUrl: './map-table.component.html',
    styleUrl: './map-table.component.scss',
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => MapTable),
            multi: true
        }
    ]
})
export class MapTable implements AfterViewInit, ControlValueAccessor {
    @Input() createNew!: (existingEntries: string[]) => Observable<MapTableEntry>;
    @Input() reportChangesOnly: boolean = false;

    private destroyRef = inject(DestroyRef);

    itemLookup: Map<string, MapTableItem> = new Map<string, MapTableItem>();
    displayedColumns: string[] = ['name', 'value', 'actions'];
    dataSource: MatTableDataSource<MapTableItem> = new MatTableDataSource<MapTableItem>();
    selectedItem!: MapTableItem;

    @ViewChildren('trigger') valueTriggers!: QueryList<CdkOverlayOrigin>;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (entries: MapTableEntry[]) => void;
    editorOpen = false;
    editorTrigger: any = null;
    editorItem!: MapTableItem;
    editorWidth = 0;
    editorOffsetX = 0;
    editorOffsetY = 0;

    private originPos: OriginConnectionPosition = {
        originX: 'center',
        originY: 'center'
    };
    private editorOverlayPos: OverlayConnectionPosition = {
        overlayX: 'center',
        overlayY: 'center'
    };
    public editorPositions: ConnectionPositionPair[] = [];

    constructor(
        private changeDetector: ChangeDetectorRef,
        private nifiCommon: NiFiCommon
    ) {}

    ngAfterViewInit(): void {
        this.initFilter();

        this.valueTriggers.changes.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            const item: MapTableItem | undefined = this.dataSource.data.find((item) => item.triggerEdit);

            if (item) {
                const valueTrigger: CdkOverlayOrigin | undefined = this.valueTriggers.find(
                    (valueTrigger: CdkOverlayOrigin) => {
                        return this.formatId(item) == valueTrigger.elementRef.nativeElement.getAttribute('id');
                    }
                );

                if (valueTrigger) {
                    // scroll into view
                    valueTrigger.elementRef.nativeElement.scrollIntoView({ block: 'center', behavior: 'instant' });

                    window.setTimeout(function () {
                        // trigger a click to start editing the new item
                        valueTrigger.elementRef.nativeElement.click();
                    }, 0);
                }

                item.triggerEdit = false;
            }
        });
    }

    initFilter(): void {
        this.dataSource.filterPredicate = (data: MapTableItem) => this.isVisible(data);
        this.dataSource.filter = ' ';
    }

    isVisible(item: MapTableItem): boolean {
        return !item.deleted;
    }

    registerOnChange(onChange: (entries: MapTableEntry[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(entries: MapTableEntry[]): void {
        this.itemLookup.clear();

        let i = 0;
        let items: MapTableItem[] = [];
        if (entries) {
            items = entries.map((entry) => {
                // create the property item
                const item: MapTableItem = {
                    entry,
                    id: i++,
                    triggerEdit: false,
                    deleted: false,
                    added: false,
                    dirty: false
                };

                // store the entry item in a map for an efficient lookup later
                this.itemLookup.set(entry.name, item);
                return item;
            });
        }

        this.setItems(items);
    }

    private setItems(items: MapTableItem[]): void {
        this.dataSource = new MatTableDataSource<MapTableItem>(items);
        this.initFilter();
    }

    newEntryClicked(): void {
        // filter out deleted properties in case the user needs to re-add one
        const existingEntries: string[] = this.dataSource.data
            .filter((item) => !item.deleted)
            .map((item) => item.entry.name);

        // create the new property
        this.createNew(existingEntries)
            .pipe(take(1))
            .subscribe((entry) => {
                const currentItems: MapTableItem[] = this.dataSource.data;

                const itemIndex: number = currentItems.findIndex(
                    (existingItem: MapTableItem) => existingItem.entry.name == entry.name
                );

                if (itemIndex > -1) {
                    const currentItem: MapTableItem = currentItems[itemIndex];
                    const updatedItem: MapTableItem = {
                        ...currentItem,
                        entry,
                        triggerEdit: true,
                        deleted: false,
                        added: true,
                        dirty: true
                    };

                    this.itemLookup.set(entry.name, updatedItem);

                    // if the user had previously deleted the entry, replace the matching entry item
                    currentItems[itemIndex] = updatedItem;
                } else {
                    const i: number = currentItems.length;
                    const item: MapTableItem = {
                        entry,
                        id: i,
                        triggerEdit: true,
                        deleted: false,
                        added: true,
                        dirty: true
                    };

                    this.itemLookup.set(entry.name, item);

                    // if this is a new entry, add it to the list
                    this.setItems([...currentItems, item]);
                }

                this.handleChanged();
            });
    }

    formatId(item: MapTableItem): string {
        return 'entry-' + item.id;
    }

    isNull(value: string): boolean {
        return value == null;
    }

    isEmptyString(value: string): boolean {
        return value == '';
    }

    hasExtraWhitespace(value: string): boolean {
        return this.nifiCommon.hasLeadTrailWhitespace(value);
    }

    openEditor(editorTrigger: any, item: MapTableItem, event: MouseEvent): void {
        if (event.target) {
            const target: HTMLElement = event.target as HTMLElement;

            // find the table cell regardless of the target of the click
            const td: HTMLElement | null = target.closest('td');
            if (td) {
                const { width } = td.getBoundingClientRect();

                this.editorPositions.pop();
                this.editorItem = item;
                this.editorTrigger = editorTrigger;
                this.editorOpen = true;

                this.editorWidth = width + 100;
                this.editorOffsetX = 8;
                this.editorOffsetY = 80;

                this.editorPositions.push(
                    new ConnectionPositionPair(
                        this.originPos,
                        this.editorOverlayPos,
                        this.editorOffsetX,
                        this.editorOffsetY
                    )
                );
            }
        }
    }

    deleteProperty(item: MapTableItem): void {
        if (!item.deleted) {
            item.entry.value = null;
            item.deleted = true;
            item.dirty = true;

            this.handleChanged();
        }
    }

    saveValue(item: MapTableItem, newValue: string | null): void {
        if (item.entry.value != newValue) {
            item.entry.value = newValue;
            item.dirty = true;

            this.handleChanged();
        }

        this.closeEditor();
    }

    private handleChanged() {
        // this is needed to trigger the filter to be reapplied
        this.dataSource._updateChangeSubscription();
        this.changeDetector.markForCheck();

        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.serializeEntries());
    }

    private serializeEntries(): MapTableEntry[] {
        const items: MapTableItem[] = this.dataSource.data;

        if (this.reportChangesOnly) {
            // only include dirty items
            return items
                .filter((item) => item.dirty)
                .filter((item) => !(item.added && item.deleted))
                .map((item) => item.entry);
        } else {
            // return all the items, even untouched ones. no need to return the deleted items though
            return items.filter((item) => !item.deleted).map((item) => item.entry);
        }
    }

    closeEditor(): void {
        this.editorOpen = false;
    }

    select(item: MapTableItem): void {
        this.selectedItem = item;
    }

    isSelected(item: MapTableItem): boolean {
        if (this.selectedItem) {
            return item.entry.name == this.selectedItem.entry.name;
        }
        return false;
    }

    protected readonly TextTip = TextTip;
}
