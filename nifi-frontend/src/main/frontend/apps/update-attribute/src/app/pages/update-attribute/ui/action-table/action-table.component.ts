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
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { NgTemplateOutlet } from '@angular/common';
import {
    CdkConnectedOverlay,
    CdkOverlayOrigin,
    ConnectionPositionPair,
    OriginConnectionPosition,
    OverlayConnectionPosition
} from '@angular/cdk/overlay';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatMenu, MatMenuItem, MatMenuModule } from '@angular/material/menu';
import { Action } from '../../state/rules';
import { v4 as uuidv4 } from 'uuid';
import { UaEditor } from '../ua-editor/ua-editor.component';

export interface ActionItem {
    id: string;
    triggerEdit: boolean;
    action: Action;
}

@Component({
    selector: 'action-table',
    templateUrl: './action-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatMenuModule,
        NifiTooltipDirective,
        NgTemplateOutlet,
        CdkOverlayOrigin,
        CdkConnectedOverlay,
        MatMenu,
        MatMenuItem,
        UaEditor
    ],
    styleUrls: ['./action-table.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => ActionTable),
            multi: true
        }
    ]
})
export class ActionTable implements AfterViewInit, ControlValueAccessor {
    @Input() isNew: boolean = false;

    private destroyRef = inject(DestroyRef);

    protected readonly TextTip = TextTip;

    displayedColumns: string[] = ['attribute', 'value', 'actions'];
    dataSource: MatTableDataSource<ActionItem> = new MatTableDataSource<ActionItem>();
    selectedItem!: ActionItem;

    @ViewChildren('trigger') valueTriggers!: QueryList<CdkOverlayOrigin>;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (actions: Action[]) => void;

    attributeEditorOpen = false;
    attributeEditorTrigger: any = null;

    valueEditorOpen = false;
    valueEditorTrigger: any = null;

    editorItem!: ActionItem;
    editorWidth = 0;
    editorOffsetX = 8;
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
        this.valueTriggers.changes.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            const item: ActionItem | undefined = this.dataSource.data.find((item) => item.triggerEdit);

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

    registerOnChange(onChange: (actions: Action[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(actions: Action[]): void {
        const actionItems: ActionItem[] = actions.map((action) => {
            // create the action item
            const item: ActionItem = {
                id: action.id,
                triggerEdit: false,
                action: {
                    ...action
                }
            };

            return item;
        });

        this.setActionItems(actionItems);
    }

    private setActionItems(actionItems: ActionItem[]): void {
        this.dataSource = new MatTableDataSource<ActionItem>(actionItems);
    }

    newActionClicked(): void {
        const currentActionItems: ActionItem[] = this.dataSource.data;

        const id = uuidv4();
        const item: ActionItem = {
            id,
            triggerEdit: true,
            action: {
                id,
                attribute: '',
                value: ''
            }
        };

        this.setActionItems([...currentActionItems, item]);
        this.handleChanged();
    }

    formatId(item: ActionItem): string {
        return 'action-' + item.id;
    }

    isEmptyString(value: string): boolean {
        return value === '';
    }

    hasExtraWhitespace(value: string): boolean {
        return this.nifiCommon.hasLeadTrailWhitespace(value);
    }

    openAttributeEditor(editorTrigger: any, item: ActionItem, event: MouseEvent): void {
        if (event.target) {
            const target: HTMLElement = event.target as HTMLElement;

            // find the table cell regardless of the target of the click
            const td: HTMLElement | null = target.closest('td');
            if (td) {
                const { width } = td.getBoundingClientRect();

                this.editorPositions.pop();
                this.editorItem = item;
                this.attributeEditorTrigger = editorTrigger;
                this.attributeEditorOpen = true;

                this.editorWidth = width;

                this.editorPositions.push(
                    new ConnectionPositionPair(
                        this.originPos,
                        this.editorOverlayPos,
                        this.editorOffsetX,
                        this.editorOffsetY
                    )
                );
                this.changeDetector.detectChanges();
            }
        }
    }

    openValueEditor(editorTrigger: any, item: ActionItem, event: MouseEvent): void {
        if (event.target) {
            const target: HTMLElement = event.target as HTMLElement;

            // find the table cell regardless of the target of the click
            const td: HTMLElement | null = target.closest('td');
            if (td) {
                const { width } = td.getBoundingClientRect();

                this.editorPositions.pop();
                this.editorItem = item;
                this.valueEditorTrigger = editorTrigger;
                this.valueEditorOpen = true;

                this.editorWidth = width;

                this.editorPositions.push(
                    new ConnectionPositionPair(
                        this.originPos,
                        this.editorOverlayPos,
                        this.editorOffsetX,
                        this.editorOffsetY
                    )
                );
                this.changeDetector.detectChanges();
            }
        }
    }

    deleteAction(item: ActionItem): void {
        const index = this.dataSource.data.indexOf(item);
        if (index > -1) {
            this.dataSource.data.splice(index, 1);
            this.handleChanged();
        }
    }

    hasActions(): boolean {
        return this.dataSource.data.length > 0;
    }

    saveActionAttribute(item: ActionItem, attribute: string): void {
        if (item.action.attribute !== attribute) {
            item.action = {
                ...item.action,
                attribute
            };

            this.handleChanged();
        }

        this.closeAttributeEditor();
    }

    saveActionValue(item: ActionItem, value: string): void {
        if (item.action.value !== value) {
            item.action = {
                ...item.action,
                value
            };

            this.handleChanged();
        }

        this.closeValueEditor();
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
        this.onChange(this.serializeActions());
    }

    private serializeActions(): Action[] {
        const actions: ActionItem[] = this.dataSource.data;
        return actions.map((item) => item.action);
    }

    closeAttributeEditor(): void {
        this.attributeEditorOpen = false;
    }

    closeValueEditor(): void {
        this.valueEditorOpen = false;
    }

    selectAction(item: ActionItem): void {
        this.selectedItem = item;
    }

    isSelected(item: ActionItem): boolean {
        if (this.selectedItem) {
            return item.id === this.selectedItem.id;
        }
        return false;
    }
}
