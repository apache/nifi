<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Theme Architecture

This directory contains the Angular Material theming layer for the NiFi frontend.

---

## Contents

- [Design System Overview](#design-system-overview)
- [Theme Stack](#theme-stack)
- [How `.darkMode` Works](#how-darkmode-works)
- [Decision Tree: Does This Need a `.darkMode` Block?](#decision-tree-does-this-need-a-darkmode-block)
- [Component Style Ownership](#component-style-ownership)
- [Styling Dynamically Created Elements (`::ng-deep`)](#styling-dynamically-created-elements-ng-deep)
- [File Structure](#file-structure)

---

## Design System Overview

The NiFi UI uses [Angular Material](https://material.angular.io/) which implements Google's
[Material Design 3](https://m3.material.io/) specification. The theme applies a minimally
customized MD3 appearance — functional and accessible, without a proprietary design token layer.

Snowflake's fork (`openflow-ui`) sits on top of this same Angular Material base but replaces the
palette values with Snowflake's Stellar design system tokens (Balto). Code flows in both directions
between the repositories, so it is important to understand where the two implementations differ.

---

## Theme Stack

```
md-ref-palette-* values (defined once in material.scss for light; swapped in .darkMode for dark)
        ↓
mat.theme() + mat.theme-overrides() → --mat-sys-* system tokens
        ↓
per-component partials: mat.*-overrides() + plain CSS using --mat-sys-*, --nf-*
        ↓
Rendered UI
```

### Entry point: `material.scss`

`material.scss` is the orchestrator. It:

1. Defines the MD3 palette via `--md-ref-palette-*` values.
2. Calls `mat.theme()` once to generate all `--mat-sys-*` system tokens.
3. Calls `generate-material-theme()` from each component partial to apply per-component overrides.

`material.scss` is loaded via `@use` in each app's `styles.scss`. Its dark theme is
self-contained — the `.darkMode {}` blocks live inside `material.scss` and inside the component
partials that need palette-index swaps. No app-level `styles.scss` needs to call any material
mixin a second time for dark mode.

### Token sources

| Token           | Example                               | Mode-aware?                                                |
| --------------- | ------------------------------------- | ---------------------------------------------------------- |
| Material system | `--mat-sys-primary`                   | Yes — `mat.theme()` resolves differently under `.darkMode` |
| NiFi-specific   | `--nf-success-default`                | Yes — rebound under `.darkMode` in `material.scss`         |
| Raw palette     | `--md-ref-palette-neutral-variant-60` | No — fixed index; must swap manually in `.darkMode`        |

---

## How `.darkMode` Works

### Why most `mat.*-overrides()` do NOT need `.darkMode` in NiFi

Angular Material's `mat.theme-overrides()` sets component tokens (`--mdc-*`, `--mat-*`) on `<body>`
in dark mode. Our `mat.*-overrides()` set them on `<html>` (`:root`). Since `<body>` is a child of
`<html>`, the `<body>` value wins the cascade.

In **openflow-ui**, this matters because our per-component colors come from `--themed-*` tokens
which auto-rebind under `.darkMode`. When those tokens are used inside `mat.*-overrides()`, the
`<body>` Material dark-theme value overrides our `:root` value unless we re-declare in `.darkMode`.

In **NiFi**, most `mat.*-overrides()` use `--mat-sys-*` tokens (e.g., `--mat-sys-secondary-container`).
These tokens are themselves already set correctly for dark mode by `mat.theme()`. Angular Material's
dark-theme defaults for those same component tokens typically equal the light-mode values when those
tokens use `--mat-sys-*` references, so no explicit `.darkMode` is needed.

### When NiFi DOES need `.darkMode`

A `.darkMode` block is needed when a `mat.*-overrides()` call hard-codes a **raw palette index**
(`--md-ref-palette-neutral-variant-N`) that must resolve to a **different index** in dark mode.

**Example** (`_snackbar.scss`):

```scss
:root {
    @include mat.snack-bar-overrides(
        (
            button-color: var(--md-ref-palette-secondary-80),
            // light: pale blue-grey
        )
    );
}

// The snackbar action button uses a fixed hex in dark mode (#004849, a dark teal)
// rather than a palette variable. This is the only way to express this design intent.
.darkMode {
    @include mat.snack-bar-overrides(
        (
            button-color: #004849
        )
    );
}
```

**Example with palette index swap** (`_checkbox.scss`):

```scss
:root {
    .tertiary-checkbox {
        @include mat.checkbox-overrides(
            (
                selected-icon-color: var(--md-ref-palette-tertiary-40),
                // light: medium brown
            )
        );
    }
}

// Tertiary palette index 40 is too dark in dark mode; index 70 (lighter) is correct.
.darkMode {
    .tertiary-checkbox {
        @include mat.checkbox-overrides(
            (
                selected-icon-color: var(--md-ref-palette-tertiary-70)
            )
        );
    }
}
```

Partials that follow this pattern: `_button.scss`, `_checkbox.scss`,
`_progress-spinner.scss`, `_snackbar.scss`.

### The DOM inheritance problem (context for code reviewers)

If you see a `.darkMode` block in a NiFi partial that mirrors its `:root` block byte-for-byte,
where every value uses `--mat-sys-*` or `--nf-*` tokens, that block is redundant and should be
removed. The `--mat-sys-*` and `--nf-*` variables are already rebound for dark mode at the
`material.scss` level, making the duplication emit the same CSS twice without changing any
computed values.

The canonical rule: **only raw palette index swaps belong in NiFi `.darkMode` blocks**.

---

## Decision Tree: Does This Need a `.darkMode` Block?

```
Is this property non-color?
(size, height, width, weight, tracking, line-height, shape,
 padding, spacing, opacity, transform, etc.)
    │
    ├─ YES → Do NOT put it in .darkMode. Same in both modes.
    │
    └─ NO (color-related) → Continue ↓

Does the value use --mat-sys-* or --nf-* tokens?
    │
    ├─ YES → These tokens resolve correctly for the current mode.
    │         No .darkMode block needed.
    │
    └─ NO → Is it a raw palette index --md-ref-palette-*-N ?
                │
                ├─ YES → Does dark mode need a DIFFERENT index?
                │           ├─ YES → Add .darkMode with the alternate index.
                │           └─ NO  → No .darkMode needed.
                │
                └─ NO (raw px, rgba, or keyword) → Evaluate case by case.
                    Plain rgba() box-shadow values do not change per mode
                    (e.g., _date-picker.scss). No .darkMode needed.
```

### Quick reference

| Scenario                                                                | `.darkMode` needed?            |
| ----------------------------------------------------------------------- | ------------------------------ |
| `mat.*-overrides()` with `--mat-sys-*` tokens                           | **No** — tokens are mode-aware |
| `mat.*-overrides()` with `--nf-*` tokens                                | **No** — tokens are mode-aware |
| `mat.*-overrides()` with raw `--md-ref-palette-*-N` that flips per mode | **Yes**                        |
| `mat.*-overrides()` with size, shape, typography                        | **No** — same in both modes    |
| Plain CSS `background-color: var(--mat-sys-surface)`                    | **No** — auto-resolves         |
| Byte-for-byte duplicate of `:root` block                                | **No** — redundant, delete it  |

---

## Component Style Ownership

Each component owns its own styles in its `*.component.scss` file. There are **no** component-theme
mixin registrations in any app's `styles.scss`.

### What lives where

| Style type                                                                   | Location                                                               |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Angular Material palette, system tokens (`--mat-sys-*`), `mat.*-overrides()` | `themes/components/_*.scss` partials, orchestrated by `material.scss`  |
| App structural resets (html/body, link styles, utility classes)              | Inlined directly in `material.scss`                                    |
| Table and Prism syntax highlighting styles                                   | `themes/components/_table.scss`, `themes/components/_prism-theme.scss` |
| Tailwind v4 `@theme` configuration                                           | `themes/components/_tailwind-theme.scss`                               |
| Component-specific layout, sizing, and color overrides                       | `*.component.scss` (co-located with the component)                     |

### `styles.scss` is a framework orchestrator only

Each app's `styles.scss` contains exactly these entries and nothing else:

```scss
@use 'libs/shared/src/assets/themes/material';
```

**There is no `html {}` block, no `@include component.generate-theme()`, and no
`.darkMode` double-call block in any `styles.scss`.** Dark-mode behavior is handled entirely
inside `material.scss` and its component partials.

---

## Styling Dynamically Created Elements (`::ng-deep`)

Angular's emulated view encapsulation adds `[_ngcontent-xxx]` attributes to elements defined in
a component's template, scoping that component's styles to those elements only. Elements created
programmatically (e.g., via D3 or `document.createElement()`) do NOT receive those attributes, so
component-scoped styles cannot reach them.

**Pattern:** Use `::ng-deep` in a component's stylesheet to pierce encapsulation:

```scss
// birdseye.component.scss
:host ::ng-deep #birdseye {
    width: 266px;
    height: 150px;
    // structural styles only
}
```

**Rules for `::ng-deep` + dark mode:**

| Situation                                     | `.darkMode` block inside `::ng-deep`?           |
| --------------------------------------------- | ----------------------------------------------- |
| Structural / layout rules only                | **No** — no color involved                      |
| Colors using `--mat-sys-*` or `--nf-*` tokens | **No** — tokens auto-resolve                    |
| Colors using raw `--md-ref-palette-*-N`       | **Yes** — add internal `.darkMode` for the swap |

NiFi's `canvas.component.scss` and `birdseye.component.scss` only contain structural layout rules
inside `::ng-deep`. Colors use `--mat-sys-*` and `--nf-*` CSS custom properties that already
resolve correctly for both light and dark mode. No `.darkMode` block is needed inside `::ng-deep`
for these components.

---

## File Structure

```
themes/
├── README.md               ← this file
├── material.scss           ← orchestrator: structural resets, palette, mat.theme(), imports all partials
├── _tailwind-theme.scss    ← Tailwind v4 @theme configuration (font sizes, etc.)
└── components/
    ├── _button.scss        ← .darkMode: .primary-icon-button (primary-40→80), .error-button (error-40→50)
    ├── _card.scss
    ├── _checkbox.scss      ← .darkMode: .tertiary-checkbox (tertiary-40→70)
    ├── _date-picker.scss
    ├── _dialog.scss
    ├── _drag-and-drop.scss
    ├── _expansion-panel.scss
    ├── _form-field.scss
    ├── _markdown.scss
    ├── _menu.scss
    ├── _option.scss        ← mat-option + multi-select-option global styles
    ├── _paginator.scss
    ├── _prism-theme.scss   ← Prism syntax highlighting (light + dark)
    ├── _progress-bar.scss
    ├── _progress-spinner.scss ← .darkMode: .tertiary-spinner (tertiary-40→70)
    ├── _searchable-overlay.scss
    ├── _sidenav.scss
    ├── _skeleton.scss
    ├── _snackbar.scss      ← .darkMode: button-color hardcoded dark hex (#004849)
    ├── _status-variant.scss
    ├── _tab.scss
    ├── _table.scss         ← listing table structural styles and theme
    ├── _tree.scss
    └── _typography.scss
```

Each partial that does **not** need a `.darkMode` block keeps only a `:root` block:

```scss
@mixin generate-material-theme() {
    :root {
        @include mat.foo-overrides((/* shape, size, --mat-sys-* colors */));
        /* plain CSS using var(--mat-sys-*) or var(--nf-*) */
    }
    // No .darkMode block — all tokens resolve correctly via mat.theme()
}
```

Partials that **do** need a `.darkMode` block document the reason inline:

```scss
@mixin generate-material-theme() {
    :root {
        .tertiary-checkbox {
            @include mat.checkbox-overrides(
                (
                    selected-icon-color: var(--md-ref-palette-tertiary-40) // light: medium brown
                )
            );
        }
    }

    // Tertiary palette index 40 is too dark on a dark surface; index 70 is the correct lighter value.
    .darkMode {
        .tertiary-checkbox {
            @include mat.checkbox-overrides(
                (
                    selected-icon-color: var(--md-ref-palette-tertiary-70)
                )
            );
        }
    }
}
```
