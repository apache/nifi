# NiFi

## General Info

This module is the primary UI for NiFi. It contains the canvas and all UI's for managing the NiFi instance. There are other modules within the codebase
that support other UIs that intergate with this. These include documentation, data viewers, advanced configuration UIs, error handling, and Registry UIs.
Overtime, these will all be modernized and possibly brought into this Nx repo to co-locate all the front end code.

In its current state it is not quite ready to replace the existing UI as there are a few features that are not implemented yet. The new UI will not
be available with the default build. However, if the build activates the `include-new-ui` profile the new UI will be built and included. On startup,
NiFi has been updated to locate the new UI and if present deploy it to a new context path (`/nf`). If the new UI is not found, nothing is deployed to
the new context path and the application functions as it does today.

One thing to note, when using the new UI running in NiFi at `/nf`, the user can log in and use the application. When logging out however, there is
a hardcoded redirect that happens from the back end which sends the user to the old UI (`/nifi`).

Once the remaining features have been implemented, the look and feel has be polished, and it is ready for release the old UI will be removed, update
the context path for the new UI to use `/nifi`, and remove the `include-new-ui` Maven profile. At that point, the logout redirection issue called out above won’t
be a problem anymore.

## Source Structure

The structure of the application is laid out in the following manner.

-   app
    -   pages
        -   flow-designer
            -   feature
            -   service
            -   state
            -   ui
        -   settings
            -   feature
            -   service
            -   state
            -   ui
        -   ...
    -   service
    -   state
    -   ui

Each page has its own directory inside `pages`. Within each page, the primary content is in `feature`, any services for that page are in `service`,
state management for the feature is in `state`, and any ui components referenced by the feature are in `ui`. The app root also contains `state`,
`service`, and `ui` which are available for use by anything in any page. Pages, however, should not access anything from any other pages.

The application leverages `ngrx` state management throughout. This includes actions and selectors for all data flow. One approach that may be
considered unique is that all dialogs and routing happen as side effects to actions dispatched by components. This leaves components less
cluttered and can focus on its purpose and not needing to deal with activated routes, dialog references, etc.

## Development server

Run `npx nx serve` for a dev server. Navigate to `http://localhost:4200/nf`. The application will automatically reload if you change any of the source files.

When accessing the UI in this manner, the login form does not work. There is some server side login request handling that does not work through
the development server. Fortunately, an authenticated user does not need to log in. So to work around this, simply log in to NiFi using the UI running in
the application first. The authentication token will be available in a cookie that is also available in the UI hosted by the development server.

## Build

Run `npx nx build` to build the project. The build artifacts will be stored in the `dist/` directory.

## Running unit tests

Run `npx nx test` to execute the unit tests via Jest.

## Linting the codebase

Run `npx nx lint` to execute lint. Additionally, run `npx nx lint:fix` to fix lint errors. Please run verify this prior to opening and PRs.

## Prettier

Run `npx nx prettier` to execute prettier to identify any formatting issues. Additionally, run `npx nx prettier:format` to fix any formatting issues.
Please run verify this prior to opening and PRs.
