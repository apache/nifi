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

import { Component, forwardRef, Input } from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { NgTemplateOutlet } from '@angular/common';
import { DocumentedType } from '../../../../../../../state/shared';
import { NifiTooltipDirective, NiFiCommon, TextTip } from '@nifi/shared';
import {
    CdkDrag,
    CdkDragDrop,
    CdkDropList,
    DragDropModule,
    moveItemInArray,
    transferArrayItem
} from '@angular/cdk/drag-drop';

@Component({
    selector: 'prioritizers',
    templateUrl: './prioritizers.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        DragDropModule,
        NgTemplateOutlet,
        NifiTooltipDirective,
        CdkDropList,
        CdkDrag
    ],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => Prioritizers),
            multi: true
        }
    ],
    styleUrls: ['./prioritizers.component.scss']
})
export class Prioritizers implements ControlValueAccessor {
    @Input() set allPrioritizers(allPrioritizers: DocumentedType[]) {
        this._allPrioritizers = [...allPrioritizers];
        this.processPrioritizers();
    }

    protected readonly TextTip = TextTip;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (selectedPrioritizers: string[]) => void;

    private _allPrioritizers: DocumentedType[] = [];

    availablePrioritizers: DocumentedType[] = [];
    selectedPrioritizers: DocumentedType[] = [];

    value!: string[];

    constructor(private nifiCommon: NiFiCommon) {}

    private processPrioritizers(): void {
        if (this._allPrioritizers && this.value) {
            const selected: DocumentedType[] = [];
            this.value.forEach((selectedPrioritizerType: string) => {
                // look up the selected prioritizer in the list of all known prioritizers
                const found = this._allPrioritizers.find(
                    (prioritizer: DocumentedType) => prioritizer.type === selectedPrioritizerType
                );
                if (found) {
                    selected.push(found);
                }
            });

            const available = this._allPrioritizers.filter((prioritizer: DocumentedType) => {
                return !selected.some(
                    (selectedPrioritizer: DocumentedType) => prioritizer.type === selectedPrioritizer.type
                );
            });

            this.selectedPrioritizers = [...selected];
            this.availablePrioritizers = [...available];
        }
    }

    registerOnChange(onChange: (selectedPrioritizers: string[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(selectedPrioritizers: string[]): void {
        this.value = [...selectedPrioritizers];
        this.processPrioritizers();
    }

    getPrioritizerLabel(entity: DocumentedType): string {
        return this.nifiCommon.getComponentTypeLabel(entity.type);
    }

    hasDescription(entity: DocumentedType): boolean {
        return !this.nifiCommon.isBlank(entity.description);
    }

    removeSelected(entity: DocumentedType, i: number): void {
        transferArrayItem(this.selectedPrioritizers, this.availablePrioritizers, i, this.availablePrioritizers.length);

        this.handleChanged();
    }

    dropAvailable(event: CdkDragDrop<DocumentedType[]>) {
        if (event.previousContainer !== event.container) {
            transferArrayItem(
                event.previousContainer.data,
                event.container.data,
                event.previousIndex,
                event.currentIndex
            );

            this.handleChanged();
        }
    }

    dropSelected(event: CdkDragDrop<DocumentedType[]>) {
        if (event.previousContainer === event.container) {
            if (event.previousIndex !== event.currentIndex) {
                moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);

                this.handleChanged();
            }
        } else {
            transferArrayItem(
                event.previousContainer.data,
                event.container.data,
                event.previousIndex,
                event.currentIndex
            );

            this.handleChanged();
        }
    }

    private handleChanged() {
        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.serializeSelectedPrioritizers());
    }

    private serializeSelectedPrioritizers(): string[] {
        return this.selectedPrioritizers.map((prioritizer) => prioritizer.type);
    }
}
