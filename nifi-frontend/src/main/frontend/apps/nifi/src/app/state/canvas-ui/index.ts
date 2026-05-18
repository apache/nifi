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

/**
 * Canvas UI State (Global)
 *
 * This state manages ONLY the UI concerns for the reusable canvas component:
 * - Transform (zoom/pan)
 * - Configuration (editor/viewer mode, features)
 *
 * Selection is managed by routes (route params are source of truth).
 * Flow data (labels, processors, connections, etc.) is managed by parent pages.
 *
 * The canvas component is parent-controlled:
 * - Takes flow data + selection via @Input (from parent)
 * - Manages UI state internally (this state)
 * - Emits events via @Output (to parent)
 */

export const canvasUiFeatureKey = 'canvasUi';

export interface Position {
    x: number;
    y: number;
}

export interface CanvasTransform {
    translate: Position;
    scale: number;
    transition?: boolean;
}

export interface CanvasConfiguration {
    features: {
        canEdit: boolean;
        canSelect: boolean;
    };
}

export interface CanvasUiState {
    transform: CanvasTransform;
    configuration: CanvasConfiguration;
}

export const initialCanvasUiState: CanvasUiState = {
    transform: {
        translate: { x: 500, y: 1700 },
        scale: 1,
        transition: false
    },
    configuration: {
        features: {
            canEdit: true,
            canSelect: true
        }
    }
};
