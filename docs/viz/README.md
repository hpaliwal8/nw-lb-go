# Interactive figures

`index.html` is the deliverable. One self-contained page, no external requests of any kind. No CDN,
no web fonts, no analytics, no build step at the far end. Every figure is hand drawn SVG driven by a
single `requestAnimationFrame` loop.

Open it straight off disk to check it (`open index.html`), then put it anywhere that serves static
files.

## Rebuilding

Sources live in `src/`. `index.html` is generated — edit the sources, not the output.

```
src/shell.html        page skeleton, masthead copy
src/style.css         design tokens (the TikZ palette, stroke weights, light/dark)
src/core.js           SVG helpers, the shared animation clock, packet motion, the hash ring model
src/diagrams/*.js     one module per figure
scripts/build-viz.py  inlines the above into docs/viz/index.html
```

```sh
make viz         # rebuild index.html
make viz-check   # rebuild, then prove in headless Chrome that every figure mounts
make viz-open    # rebuild and open in a browser
```

Figure order on the page is the `FIGURES` list in `scripts/build-viz.py`. Three figures are built.
Three more modules stay in `src/diagrams/` unused (`lifecycle`, `breaker`, `saturation`). Add a name
back to that list to bring one in.

## Hosting

The page is a single file, so every option below is a copy.

**Any static host.** Netlify, Vercel, Cloudflare Pages, S3 + CloudFront, or a directory on your own
server. Drop `index.html` in and you are done. Netlify and Cloudflare will take a drag-and-drop of
the file itself.

**GitHub Pages.** This repo has no remote yet. Once it has one:

```sh
git add -A && git commit -m "Add interactive figures"
git push -u origin main
```

Then in the repository settings, under Pages, set the source to `main` and the folder to `/docs`.
The page lands at `https://<user>.github.io/<repo>/viz/`. A `CNAME` file in `docs/` points a custom
domain at it.

**Embedded in an existing portfolio.** Because it is one file with no globals beyond `NWLB` and no network
calls, an `<iframe>` is safe and isolates the styling completely.

```html
<iframe src="/nw-lb-go-figures.html" style="width:100%;height:900px;border:0" loading="lazy"
        title="nw-lb-go interactive figures"></iframe>
```

If you would rather inline it into a page you already own, take `src/style.css` and the built
`<script>` block and mount into your own container — `NWLB.mountAll(el)` is the only entry point.

## Notes for a reader

The page is light only, on purpose. It is a technical drawing on paper, and the ink weights are
tuned for that ground. It paints its own background and declares `color-scheme: light`, so it keeps
its appearance inside a dark portfolio rather than borrowing one. It respects
`prefers-reduced-motion` by slowing packet spawning rather than freezing, so the controls still
demonstrate the behaviour.

Figures are only animated while scrolled into view, so the page costs about as much as the one
figure you are looking at, regardless of how many exist.
