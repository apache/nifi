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
import { ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatCheckboxModule } from '@angular/material/checkbox';

import { Relationship } from '../../../../../../state/flow';
import { NiFiCommon } from '@nifi/shared';

export interface RelationshipItem {
    relationshipName: string;
    selected: boolean;
    available: boolean;
}

@Component({
    selector: 'source-processor',
    templateUrl: './source-processor.component.html',
    styleUrls: ['./source-processor.component.scss'],
    imports: [MatCheckboxModule, FormsModule],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => SourceProcessor),
            multi: true
        }
    ]
})
export class SourceProcessor implements ControlValueAccessor {
    @Input() set processor(processor: any) {
        if (processor) {
            this.name = processor.component.name;
            this.relationships = processor.component.relationships;
            this.processRelationships();
        }
    }
    @Input() groupName!: string;

    isDisabled = false;
    isTouched = false;
    onTouched: (() => void) | null = null;
    onChange: ((selectedRelationships: string[]) => void) | null = null;

    name!: string;
    relationships!: Relationship[];

    relationshipItems!: RelationshipItem[];
    selectedRelationships!: string[];

    constructor(private nifiCommon: NiFiCommon) {}

    processRelationships(): void {
        if (this.relationships) {
            if (this.nifiCommon.isEmpty(this.selectedRelationships)) {
                this.relationshipItems = this.relationships.map((relationship) => {
                    return {
                        relationshipName: relationship.name,
                        selected: this.relationships.length === 1,
                        available: true
                    };
                });

                this.considerDefaultSelection();
            } else {
                this.relationshipItems = this.relationships.map((relationship) => {
                    return {
                        relationshipName: relationship.name,
                        selected: this.selectedRelationships.includes(relationship.name),
                        available: true
                    };
                });

                const unavailableRelationships: string[] = this.selectedRelationships.filter(
                    (selectedRelationship) =>
                        !this.relationships.some((relationship) => relationship.name == selectedRelationship)
                );
                unavailableRelationships.forEach((unavailableRelationship) => {
                    this.relationshipItems.push({
                        relationshipName: unavailableRelationship,
                        selected: true,
                        available: false
                    });
                });
            }
        }
    }

    considerDefaultSelection(): void {
        const callbacksConfigured: boolean = this.onChange != null && this.onTouched != null;
        const autoSelected: boolean =
            this.relationships?.length === 1 && this.nifiCommon.isEmpty(this.selectedRelationships);

        if (callbacksConfigured && autoSelected) {
            this.handleChanged();
        }
    }

    registerOnChange(onChange: (selectedPrioritizers: string[]) => void): void {
        this.onChange = onChange;
        this.considerDefaultSelection();
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
        this.considerDefaultSelection();
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(selectedRelationships: string[]): void {
        this.selectedRelationships = [...selectedRelationships];
        this.processRelationships();
    }

    handleChanged() {
        if (this.onTouched && this.onChange) {
            // mark the component as touched if not already
            if (!this.isTouched) {
                this.isTouched = true;
                this.onTouched();
            }

            // emit the changes
            this.onChange(this.serializeSelectedRelationships());
        }
    }

    private serializeSelectedRelationships(): string[] {
        return this.relationshipItems.filter((item) => item.selected).map((item) => item.relationshipName);
    }
}
