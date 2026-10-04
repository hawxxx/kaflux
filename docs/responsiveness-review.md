# Responsiveness and interaction review

Reviewed locally on 2026-10-04 using Chromium through agent-browser. Frontend: http://localhost:5173; backend: `make dev` (simulator mode). Both were left running. This review covers simulated data, not live Kafka, authenticated sessions, or real mutation execution.

## Findings

| Priority | Finding | Reproduction and evidence | Suggested change |
| --- | --- | --- | --- |
| P2 | Narrow header clips controls | At 320×844, open Reassignments. Document width becomes 345px and the theme control extends to x=345. See `../output/playwright/kaflux-320-reassignments.png`. Header styles are in `frontend/src/styles.css:8`. | Allow breadcrumb truncation/shrinking and adapt the demo badge/actions at narrow widths. |
| P2 | Mobile navigation lacks keyboard dismissal and focus management | At 390×844, open navigation, press Escape, then Tab. Drawer remains open and focus moves to global search behind it. The closed sidebar still has buttons with tabIndex 0. Drawer implementation: `frontend/src/main.tsx:38`; hiding uses only a transform in `frontend/src/styles.css:8`. | Provide drawer semantics, Escape dismissal, focus entry/return, and make closed navigation inert. |
| P2 | Mobile global search has no accessible name | At 390px, the accessibility snapshot reports an unnamed button. Its visible text is empty and aria-label is absent; the icon is aria-hidden. `frontend/src/main.tsx:38` and responsive rules in `frontend/src/styles.css:8`. | Give the search button a persistent accessible label. |
| P3 | Tablet overview is cramped | At 768×900, fixed sidebar plus four stat columns leave narrow cards; stored-data units and several labels wrap. See `../output/playwright/kaflux-tablet.png`. | Switch statistics to two columns before the mobile breakpoint, based on available content width. |
| P3 | Small phone touch targets | At 390px, global search measures 22px tall. Several header controls and table checkboxes are similarly small. | Expand touch hit areas while retaining compact icons. |

## Verified behavior

- Desktop overview at 1440×900 and phone/tablet screenshots in dark and light themes.
- Layout checks at 320, 390, 768, 1024, and 1440px. Overview, topics, messages, metrics, balance, reassignments, access control, and settings were inspected; consumer groups, brokers, and integration workflows were not exercised in depth.
- Topic filtering updates the URL. Clearing with Ctrl+A and Backspace restores the empty query. An automation `fill` with an empty string did not reliably clear the controlled field; this was excluded from product findings.
- Phone topic tables become readable cards; create-topic form fits within the 390px viewport and Escape closes it.
- Mobile navigation opens and selecting Messages closes it.
- Workspace search opens and Escape closes it; theme switching updates the rendered theme.
- Selecting orders.created and Fetch loads records and payload controls; live tail toggles to Pause and back to Start.
- Unconfigured overview metrics show an explanatory unavailable state; no JavaScript page errors were reported during the inspected interactions.

## Limits and artifacts

The initial review did not change application source, build, or run automated suites. Its findings were subsequently remediated as described below. Production-scale data and real broker/identity-provider workflows remain outside local simulator verification.

Screenshots are in `output/playwright/`: desktop overview, mobile topic dialog/cards/navigation/message controls/light theme, narrow reassignment header, and tablet overview. The original modified `AGENTS.md` was preserved. No `.beads` database or `graphify-out/graph.json` exists in this checkout.

## Remediation

All five review findings are addressed. Phone navigation is now a labeled modal drawer with Escape/backdrop/close-button dismissal, focus trapping and return, an inert hidden sidebar, and resize cleanup. Opening workspace search from the drawer closes navigation first. The phone header truncates breadcrumbs and adapts its demo badge so controls fit at 320px; header controls have named 44px touch targets. Phone actions are taller and table checkboxes are enlarged. Tablet overview statistics use two columns through 1100px.

Regression coverage in `frontend/tests/responsiveness.spec.ts` verifies the 320/390px header, keyboard navigation, hidden-navigation exclusion, page selection, backdrop dismissal, search shortcut, desktop resize, tablet columns, delayed loading, unavailable metadata, and Refresh recovery. CI runs these checks with session and shared-topic-URL regressions.

The full frontend unit suite passed (40 tests). Source and lockfiles in a temporary Linux validation directory were byte-compared with the checkout because Windows-mounted builds exceeded short tool timeouts. The final production bundle builds successfully; Rollup took 2m12s and emitted the existing large-chunk warning. All 25 Playwright workflows passed against the final compiled bundle served at http://localhost:8080 with backend CSP/security headers, including the corrected logout route. Development remains available at http://localhost:5173. Changes are uncommitted.

Updated production screenshots: `../output/playwright/fixed-header-320.png`, `../output/playwright/fixed-navigation-mobile.png`, and `../output/playwright/fixed-overview-tablet.png`. See `security-review.md` for security findings, remediations, and deployment limits.

## Follow-up review

A second agent-browser pass on 2026-10-04 covered every navigation page at 1440, 1024, 768, 390 and 320px. It checked for overflow, unnamed controls, small touch targets and console errors.

- The throughput chart's y-axis showed "0,000" on every tick because the default uPlot axis was too narrow for the byte values. Axes and hover values are now formatted from the catalog unit, for example `24.0 MiB/s`. The series takes the metric's name, and axis and grid colours follow the theme. A unit test covers the formatter.
- Pending metrics said "A configured datasource is required" and waited 30 seconds before retrying. Pending queries now explain that the first samples are being collected and poll every 3 seconds until data arrives.
- The "Last hour" label had a dropdown chevron but was not a control, so the chevron was removed. The uPlot hover legend is hidden below 640px.
- At 768px the Topics toolbar clipped the Export action. Toolbars now wrap, and the demo badge and search shortcut no longer break across lines.
- Several touch targets were too small: the cluster selector (14px tall), "Analyze balance" (15px), consumer-group links (13px) and reassignment checkboxes (14px). They are now at least 32–44px on phones.
- The sidebar demo-session note sat flush against the viewport edge. It now has padding and muted styling.

Verification: 43 Vitest tests passed, and the production build passed with the existing large-chunk warning. All 25 Playwright workflows passed against the compiled bundle served by the patched backend. The repeat sweep reported no clipped content. The only console error was a duplicate `createRoot` warning caused by Vite hot reload during editing, which does not appear in production builds.
