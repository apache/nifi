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
    Directive,
    EmbeddedViewRef,
    Input,
    OnChanges,
    SimpleChanges,
    TemplateRef,
    ViewContainerRef,
    inject
} from '@angular/core';

@Directive({
    selector: '[recreateView]',
    standalone: true
})
export class RecreateViewDirective implements OnChanges {
    private templateRef = inject<TemplateRef<unknown>>(TemplateRef);
    private viewContainer = inject(ViewContainerRef);

    @Input('recreateView') key: unknown;

    viewRef: EmbeddedViewRef<unknown> | null = null;

    ngOnChanges(changes: SimpleChanges): void {
        if (changes['key']) {
            this.destroyView();
            this.createView();
        }
    }

    private createView() {
        this.viewRef = this.viewContainer.createEmbeddedView(this.templateRef);
    }

    private destroyView() {
        if (this.viewRef) {
            this.viewRef.destroy();
            this.viewRef = null;
        }
    }
}
