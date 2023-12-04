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
import { AsyncPipe, NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { RouterLink } from '@angular/router';
import { NiFiCommon } from '../../../../../../../service/nifi-common.service';
import { DocumentedType, TextTipInput } from '../../../../../../../state/shared';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import {
    DragDropModule,
    CdkDrag,
    CdkDragDrop,
    CdkDropList,
    moveItemInArray,
    transferArrayItem
} from '@angular/cdk/drag-drop';

@Component({
    selector: 'prioritizers',
    standalone: true,
    templateUrl: './prioritizers.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        DragDropModule,
        NgIf,
        NgTemplateOutlet,
        CdkOverlayOrigin,
        CdkConnectedOverlay,
        RouterLink,
        AsyncPipe,
        NifiTooltipDirective,
        CdkDropList,
        CdkDrag,
        NgForOf
    ],
    styleUrls: ['./prioritizers.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => Prioritizers),
            multi: true
        }
    ]
})
export class Prioritizers implements ControlValueAccessor {
    @Input() set allPrioritizers(allPrioritizers: DocumentedType[]) {
        this._allPrioritizers = [...allPrioritizers];
        this.processPrioritizers();
    }

    protected readonly TextTip = TextTip;

    isDisabled: boolean = false;
    isTouched: boolean = false;
    onTouched!: () => void;
    onChange!: (selectedPrioritizers: string[]) => void;

    _allPrioritizers: DocumentedType[] = [];

    availablePrioritizers: DocumentedType[] = [];
    selectedPrioritizers: DocumentedType[] = [];

    value!: string[];

    constructor(private nifiCommon: NiFiCommon) {}

    private processPrioritizers(): void {
        this.availablePrioritizers = [];
        this.selectedPrioritizers = [];

        if (this._allPrioritizers && this.value) {
            this._allPrioritizers.forEach((prioritizer) => {
                const selected: boolean = this.value.some(
                    (selectedPrioritizerType) => prioritizer.type == selectedPrioritizerType
                );
                if (selected) {
                    this.selectedPrioritizers.push(prioritizer);
                } else {
                    this.availablePrioritizers.push(prioritizer);
                }
            });
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
        return this.nifiCommon.substringAfterLast(entity.type, '.');
    }

    hasDescription(entity: DocumentedType): boolean {
        return !this.nifiCommon.isBlank(entity.description);
    }

    getDescriptionTipData(entity: DocumentedType): TextTipInput {
        return {
            // @ts-ignore
            text: entity.description
        };
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
