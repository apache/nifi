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

import { Component, computed, input, output, signal } from '@angular/core';
import { MatIconButton } from '@angular/material/button';
import { OverlappingConnectionGroup } from '../overlap-detection.utils';

@Component({
    selector: 'overlapping-connections-banner',
    standalone: true,
    imports: [MatIconButton],
    templateUrl: './overlapping-connections-banner.component.html',
    styleUrls: ['./overlapping-connections-banner.component.scss']
})
export class OverlappingConnectionsBannerComponent {
    overlappingGroups = input<OverlappingConnectionGroup[]>([]);
    navigateToGroup = output<OverlappingConnectionGroup>();

    private dismissedKey = signal<string | null>(null);

    // the banner is shown again when the set of overlapping connections changes
    private currentKey = computed(() =>
        this.overlappingGroups()
            .flatMap((group) => group.connectionIds)
            .join(',')
    );

    visible = computed(() => this.overlappingGroups().length > 0 && this.currentKey() !== this.dismissedKey());

    dismiss(): void {
        this.dismissedKey.set(this.currentKey());
    }

    showOverlap(overlap: OverlappingConnectionGroup): void {
        this.navigateToGroup.emit(overlap);
    }
}
