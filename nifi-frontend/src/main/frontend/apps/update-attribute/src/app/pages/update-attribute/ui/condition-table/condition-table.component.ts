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
import { Condition } from '../../state/rules';
import { v4 as uuidv4 } from 'uuid';
import { UaEditor } from '../ua-editor/ua-editor.component';

export interface ConditionItem {
    id: string;
    triggerEdit: boolean;
    condition: Condition;
}

@Component({
    selector: 'condition-table',
    templateUrl: './condition-table.component.html',
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
    styleUrls: ['./condition-table.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => ConditionTable),
            multi: true
        }
    ]
})
export class ConditionTable implements AfterViewInit, ControlValueAccessor {
    @Input() isNew: boolean = false;

    private destroyRef = inject(DestroyRef);

    protected readonly TextTip = TextTip;

    displayedColumns: string[] = ['expression', 'actions'];
    dataSource: MatTableDataSource<ConditionItem> = new MatTableDataSource<ConditionItem>();
    selectedItem!: ConditionItem;

    @ViewChildren('trigger') valueTriggers!: QueryList<CdkOverlayOrigin>;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (conditions: Condition[]) => void;

    editorOpen = false;
    editorTrigger: any = null;
    editorItem!: ConditionItem;
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
            const item: ConditionItem | undefined = this.dataSource.data.find((item) => item.triggerEdit);

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

    registerOnChange(onChange: (conditions: Condition[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(conditions: Condition[]): void {
        const conditionItems: ConditionItem[] = conditions.map((condition) => {
            // create the condition item
            const item: ConditionItem = {
                id: condition.id,
                triggerEdit: false,
                condition: {
                    ...condition
                }
            };

            return item;
        });

        this.setConditionItems(conditionItems);
    }

    private setConditionItems(conditionItems: ConditionItem[]): void {
        this.dataSource = new MatTableDataSource<ConditionItem>(conditionItems);
    }

    newConditionClicked(): void {
        const currentConditionItems: ConditionItem[] = this.dataSource.data;

        const id = uuidv4();
        const item: ConditionItem = {
            id,
            triggerEdit: true,
            condition: {
                id,
                expression: ''
            }
        };

        this.setConditionItems([...currentConditionItems, item]);
        this.handleChanged();
    }

    formatId(item: ConditionItem): string {
        return 'condition-' + item.id;
    }

    isEmptyString(value: string): boolean {
        return value === '';
    }

    hasExtraWhitespace(value: string): boolean {
        return this.nifiCommon.hasLeadTrailWhitespace(value);
    }

    openEditor(editorTrigger: any, item: ConditionItem, event: MouseEvent): void {
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

    deleteCondition(item: ConditionItem): void {
        const index = this.dataSource.data.indexOf(item);
        if (index > -1) {
            this.dataSource.data.splice(index, 1);
            this.handleChanged();
        }
    }

    hasConditions(): boolean {
        return this.dataSource.data.length > 0;
    }

    saveConditionValue(item: ConditionItem, expression: string | null): void {
        if (expression && item.condition.expression != expression) {
            item.condition = {
                ...item.condition,
                expression
            };

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
        this.onChange(this.serializeConditions());
    }

    private serializeConditions(): Condition[] {
        const conditions: ConditionItem[] = this.dataSource.data;
        return conditions.map((item) => item.condition);
    }

    closeEditor(): void {
        this.editorOpen = false;
    }

    selectCondition(item: ConditionItem): void {
        this.selectedItem = item;
    }

    isSelected(item: ConditionItem): boolean {
        if (this.selectedItem) {
            return item.id === this.selectedItem.id;
        }
        return false;
    }
}
