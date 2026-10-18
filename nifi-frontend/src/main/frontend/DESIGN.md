# Regenerate when the NiFi color palette or component theming changes.
# Color values sourced from libs/shared/src/assets/themes/material.scss.
# Structural values (component radii) from libs/shared/src/assets/themes/components/_*.scss.
---
name: Apache NiFi
description: >
  Default Angular Material MD3 with NiFi-specific teal palette and canvas tokens.
  Values sourced from libs/shared/src/assets/themes/material.scss and
  components/_*.scss.

colors:
  primary: "#004849"
  primary_container: "#e3e3e3"
  on_primary: "#ffffff"
  secondary: "#abbdc5"
  secondary_container: "#cbd8dd"
  background: "#fafafa"
  surface: "#ffffff"
  surface_dim: "#e3e3e3"
  surface_low: "#fafafa"
  surface_container: "#f4f4f4"
  surface_high: "#e3e3e3"
  surface_highest: "#e3e3e3"
  text_primary: "#303030"
  text_secondary: "#666666"
  text_on_primary: "#ffffff"
  border: "#d8d8d8"
  border_variant: "#d8d8d8"
  success: "#31975b"
  success_bg: "#bdf8e9"
  error: "#ba554a"
  error_variant: "#eb7071"
  caution: "#cf9338"
  neutral: "#666666"
  disabled: "#d8d8d8"
  canvas_bg: "#e5ebed"
  canvas_banner: "#12121214"
  canvas_odd_row: "#12121206"
  canvas_even_row: "#fafafa"

typography:
  body-sm:
    fontFamily: Inter
    fontSize: 12px
    fontWeight: 400
  body:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: 400
  body-lg:
    fontFamily: Inter
    fontSize: 16px
    fontWeight: 400
  title:
    fontFamily: Inter
    fontSize: 16px
    fontWeight: 500
  headline:
    fontFamily: Inter
    fontSize: 24px
    fontWeight: 400

spacing:
  xs: 4px
  sm: 8px
  md: 12px
  lg: 16px
  xl: 24px
  "2xl": 32px

rounded:
  sm: 4px
  md: 6px
  lg: 12px

components:
  button:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.text_on_primary}"
    rounded: "{rounded.sm}"
  form-field:
    rounded: "{rounded.sm}"
  dialog:
    rounded: "{rounded.md}"
  card:
    rounded: "{rounded.lg}"
  canvas:
    backgroundColor: "{colors.canvas_bg}"
---

# Apache NiFi Frontend — Design System Reference

> **Repository**: `nifi` (Apache NiFi)  
> **Design spec**: Angular Material MD3 + NiFi-specific canvas tokens  
> **Implementation stack**: Angular + Angular Material (MD3) + Tailwind CSS v4 + Font Awesome 4.7

This document describes the visual identity and design language of the Apache NiFi frontend. It is the source of truth for AI coding agents performing UI work in this repository.

---

## Overview

The NiFi frontend implements **Angular Material Design 3** with a custom color palette tuned to the NiFi brand: a teal primary (`#004849`), grey neutrals, and a distinct canvas environment for the flow designer.

Theming is done by mapping NiFi's palette hex values to Angular Material's `--md-ref-palette-*` system in `libs/shared/src/assets/themes/material.scss`, then calling `mat.theme-overrides()`. On top of this, NiFi-specific `--nf-*` CSS custom properties carry canvas and status semantics that have no Angular Material equivalent.

---

## Design Language

### Brand Voice (Visual)
- **Information-dense**: The flow canvas is the primary interface; every chrome element should feel subordinate to the flow graph.
- **Functional clarity**: Teal (`#004849`) communicates action; greys communicate structure; status colors are strictly semantic.
- **Professional, minimal**: Standard MD3 interaction patterns (ripples, state layers) are preserved throughout.

### Visual Principles
- **Teal-on-white primary palette** — `#004849` teal drives all primary actions against white/near-white surfaces.
- **Grey neutrals for structure** — borders (`#d8d8d8`), surfaces (`#f4f4f4`), and disabled states use the neutral scale.
- **Canvas vs chrome** — the flow canvas uses `#e5ebed` background and `--nf-*` CSS variables distinct from the app chrome.

---

## Colors

### MD3 Palette

All component colors come from Angular Material's MD3 system token layer (`--mat-sys-*`), populated by `mat.theme-overrides()` in `material.scss` using the palette values defined on `:root`. The palette is fully overridden under `.darkMode {}` on `<body>`.

### Light Mode Key Values

| Semantic role | CSS variable | Hex value |
|---------------|-------------|-----------|
| Primary | `--mat-sys-primary` | `#004849` |
| Primary container | `--mat-sys-primary-container` | `#e3e3e3` |
| Background | `--mat-sys-background` | `#fafafa` |
| Surface | `--mat-sys-surface` | `#ffffff` |
| On-surface | `--mat-sys-on-surface` | `#303030` |
| Outline | `--mat-sys-outline` | `#d8d8d8` |
| Error | `--mat-sys-error` | `#ba554a` |

### NiFi-Specific Tokens

Canvas and status colors use `--nf-*` variables defined in `material.scss`. Use these for all canvas and status work:

| Token | Light value | Purpose |
|-------|------------|---------|
| `--nf-canvas-background` | `#e5ebed` | Flow canvas drawing surface |
| `--nf-success-default` | `#31975b` | Running processor / success state |
| `--nf-success-default-background` | `#bdf8e9` | Success state background |
| `--nf-caution-default` | `#cf9338` | Caution / warning state |
| `--nf-error-variant` | `#eb7071` | Stopped-run status color |
| `--nf-neutral` | `#666666` | Neutral status |
| `--nf-disabled` | `#d8d8d8` | Disabled elements |
| `--nf-odd` | `#12121206` | Alternating table row (odd) |
| `--nf-even` | `#fafafa` | Alternating table row (even) |
| `--nf-banner` | `#12121214` | Banner / highlighted row |

### Dark Mode

Under `.darkMode {}` on `<body>`, the palette variables are fully overridden:

| Role | Dark value |
|------|-----------|
| Primary | `#cbd8dd` |
| Background | `#121212` |
| Surface | `#303030` |
| On-surface | `#e3e3e3` |
| Canvas bg | `#0d1411` |

---

## Typography

**Font family**: Inter (declared via `--md-ref-typeface-plain: Inter` and `--md-ref-typeface-brand: Inter`).

NiFi uses Angular Material's MD3 default type scale:

| MD3 role | Size | Weight | NiFi use |
|----------|------|--------|---------|
| `body-medium` | 14px | 400 | Default text, property values |
| `body-large` | 16px | 400 | Form inputs |
| `label-medium` | 12px | 500 | Table headers, labels |
| `title-medium` | 16px | 500 | Card titles, dialog headings |
| `headline-small` | 24px | 400 | Page headings |

Access via Angular Material typography system — do not hardcode font sizes.

---

## Layout

NiFi uses **Tailwind CSS v4** for layout and spacing in templates:

| Tailwind class | Value | Common use |
|---------------|-------|-----------|
| `gap-1` | 4px | Tight icon-label gap |
| `gap-2` | 8px | Standard item gap |
| `gap-3` | 12px | Form field spacing |
| `gap-4` | 16px | Section padding |
| `gap-6` | 24px | Dialog / card padding |
| `p-2` | 8px | Button padding |
| `p-4` | 16px | Panel padding |

### Layout Rules
- Use Tailwind flex/grid utilities — avoid custom spacing SCSS.
- Host element layout via `@Component host: { class: '...' }` — never `:host { display: }`.
- Fill height with `flex-1 min-h-0` — never `height: calc(100vh - Npx)`.

---

## Shapes

| Surface type | Radius | Source |
|-------------|--------|--------|
| Buttons | 4px | `filled-container-shape: 4px` in `_button.scss` |
| Inputs | 4px | MD3 default extra-small (no override in `_form-field.scss`) |
| Dialogs | 6px | `container-shape: 6px` in `_dialog.scss` |
| Cards | 12px | MD3 default medium (no override in `_card.scss`) |

---

## Components

### Buttons

NiFi uses the full Angular Material button directive set:

| Directive | NiFi use |
|-----------|---------|
| `mat-flat-button` | Primary / confirm actions (filled teal) |
| `mat-button` | Cancel / dismiss / secondary text actions |
| `mat-stroked-button` | Outlined secondary actions |
| `mat-icon-button` | Icon-only actions |

```html
<!-- Primary -->
<button mat-flat-button color="primary">Start</button>

<!-- Cancel -->
<button mat-button mat-dialog-close>Cancel</button>

<!-- Icon-only -->
<button mat-icon-button>
  <i class="fa fa-pencil"></i>
</button>
```

### Icons

NiFi uses **Font Awesome 4.7** (`<i class="fa fa-*">`).

```html
<i class="fa fa-play"></i>    <!-- start -->
<i class="fa fa-stop"></i>    <!-- stop -->
<i class="fa fa-trash"></i>   <!-- delete -->
<i class="fa fa-search"></i>  <!-- search -->
<i class="fa fa-cog"></i>     <!-- settings -->
```

### Spinner

Use `NifiSpinnerDirective` (from `@nifi/shared`):

```html
<div [nifiSpinner]="isLoading">content</div>
```

### Forms

Angular Material form fields with `appearance="outline"`:

```html
<mat-form-field appearance="outline">
  <mat-label>Label</mat-label>
  <input matInput />
</mat-form-field>
```

Style via `mat.form-field-overrides()` in `_app.scss` using `--nf-*` and `--mat-sys-*` tokens — never with hardcoded hex in component SCSS.

---

## Code Editor Colors

NiFi includes CodeMirror 6 for property expression editing. Editor theme tokens are defined in `material.scss`.

### Light mode syntax highlighting

| Token | Value | Language role |
|-------|-------|--------------|
| `--editor-keyword` | `#085bd7` | Keywords |
| `--editor-string` | `#860112` | String literals |
| `--editor-function` | `#087959` | Functions |
| `--editor-comment` | `#5d6a85` | Comments |
| `--editor-type` | `#b67901` | Types / control |
| `--editor-variable-name` | `#002c6e` | Variables |
| `--editor-selected-background` | `#d6e6ff` | Selection |

---

## Implementation Notes (Angular)

### Technology Stack

| Concern | Technology | Notes |
|---------|-----------|-------|
| Framework | Angular (NgModule + standalone) | Mix of NgModule feature modules and standalone components |
| Component library | Angular Material MD3 | Standard MD3 palette |
| Icons | Font Awesome 4.7 | `<i class="fa fa-*">` |
| CSS | Tailwind CSS v4 + Angular Material SASS | No third-party token packages |
| State management | NgRx | Actions, effects, selectors, reducers |
| Build | Nx monorepo | `nx run <project>:<target>` |
| Routing | Hash routing (`useHash: true`) | |

### Key File Locations

| File | Purpose |
|------|---------|
| `libs/shared/src/assets/themes/material.scss` | MD3 palette + `mat.theme-overrides()` + `--nf-*` definitions |
| `libs/shared/src/assets/styles/_app.scss` | Global styles, `generate-material-theme()` mixin |
| `apps/nifi/src/app/` | Main app — pages, state, services |
| `libs/shared/src/` | Shared library (`@nifi/shared`) |

### Import Alias

Always use `@nifi/shared` — never a relative path to the shared library:

```typescript
import { NifiSpinnerDirective } from '@nifi/shared';
```

### Theming Pattern

```scss
// material.scss
:root {
  --md-ref-palette-primary-40: #004849;

  @include mat.theme-overrides((
    primary: var(--md-ref-palette-primary-40),
    // ...
  ));
}

.darkMode {
  --md-ref-palette-primary-40: #cbd8dd;

  @include mat.theme-overrides((
    primary: var(--md-ref-palette-primary-40),
    // ...
  ));
}
```

Component-specific overrides go in `_app.scss` via `mat.*-overrides()` mixins:

```scss
@include mat.button-overrides((
  filled-container-color: var(--mat-sys-primary),
));
```

### No Hardcoded Colors in Components

Use `--mat-sys-*` and `--nf-*` CSS custom properties in component SCSS — never hardcode hex values:

```scss
// ✅ Correct
color: var(--mat-sys-on-surface);
background-color: var(--nf-canvas-background);

// ❌ Wrong
color: #303030;
background-color: #e5ebed;
```


## Opt-in Modern Designer Workspace

The designer's **Modern workspace** control enables a scoped shell migration.
Classic presentation remains the default. The workspace preference now persists
across authenticated routes and reloads through the existing browser Storage service.
Canvas presentation continues to migrate separately. The screenshot reference guides hierarchy and palette;
metrics always use the verified API contracts.

Palette source: `apps/nifi/src/app/ui/common/modern-workspace/_workspace-tokens.scss`.
Keep all new palette literals in that file. Workspace components consume semantic
`--workspace-*` tokens, mapped locally to existing `--mat-sys-*` and `--nf-*`
properties. The shared MD3 palette and the other frontend applications are unchanged.
Both light and `.darkMode` palettes are supported; CDK overlays outside the workspace
continue using the existing application theme during this migration.

| Role | Light | Dark |
|---|---|---|
| Canvas background | `#f2f6fb` | `#09151f` |
| Chrome | `#ffffff` | `#0d1b27` |
| Card | `#ffffff` | `#142433` |
| Primary text | `#172b43` | `#e6eef8` |
| Secondary text | `#4d6179` | `#aabed3` |
| Accent | `#0869b4` | `#60baff` |
| Border | `#c7d4e2` | `#334e65` |
| Success | `#137342` | `#44df98` |
| Warning | `#8c570c` | `#ffc56b` |
| Error | `#b62c40` | `#ff7d8d` |

The shell preserves the existing authenticated header, creation toolbar, global
menu, Flow Analysis, search, breadcrumbs and D3 geometry. Sidebar destinations use
the same permission fields as the global menu. A sidebar is not an authorization
boundary; existing guards and the server remain authoritative.

At widths up to 1100px the sidebar collapses to named icon controls. At widths up
to 1000px the details drawer overlays the canvas and has an explicit close action,
so it does not force the diagram into a narrow column. The operational panel has a
bounded scroll area and sticky controls; the narrow header status strip scrolls
horizontally. Focus, status text and icons supplement semantic colors.

This shell does not introduce a health score, error-rate definition, time-series
store, environment switcher, animation loop or additional polling. Those features
require their own contracts and qualification before appearing as runtime facts.

The common `WorkspacePageComponent` now supplies one authenticated header and the
optional sidebar for operational and configuration routes, including documentation.
Each route retains its original BannerText owner. Designer retains its specialized
creation toolbar and status strip. Login, logout-complete, error, 404, content viewer
and separate extension applications retain their original utility layouts. The
preference is `workspace-presentation` with the recognized `modern-v1` value; unknown
formats default to classic, and selecting classic removes the preference. It is a
browser presentation preference, not a NiFi parameter or server configuration.

The opt-in Flow Designer now scopes its SVG presentation through
`pages/flow-designer/ui/canvas/_modern-canvas.scss`. Processor cards retain their
352 × 128 bounds and connection labels their 240px width. The existing designer
`ProcessorManager` and `ConnectionManager` add hidden SVG status/header text;
modern styles reveal it without changing the classic layout or event handlers.
Use `visibility`, rather than `display`, for these extra text elements so D3 can
measure and truncate their text before the user switches modes. The common canvas
renderers used by connector configuration are a separate rendering path and remain
unchanged. Bundle metadata remains in the status tooltip; relationship names are
included in the queue header tooltip, with the classic labels still available.

Queue header percentages use valid server-reported count/byte utilization values
and show their maximum. Missing/invalid data displays an em dash. Colors retain
the existing canvas thresholds (61–85 warning, above 85 high); these are visual
categories, not a claim about scheduler enforcement. Count/byte bars and their
existing threshold tooltips remain available. No geometry constants, API payloads,
flow execution behavior or polling ownership change as part of this presentation.
