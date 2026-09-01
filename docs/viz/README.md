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

The page is one file with no external requests, so hosting it is a copy. `public/index.html` is the
deploy root and holds nothing else, so no other file in the repo is ever served. `make viz` rewrites
it along with the other outputs.

### Vercel

`vercel.json` is already set up: no framework, no build step, `public` as the output directory.

```sh
vercel login          # once, opens a browser
make viz-deploy       # builds, then deploys to production
```

The first deploy asks which scope to use and what to call the project. Accept the defaults for the
rest, since `vercel.json` already answers the framework and output questions.

To deploy on every push instead, commit and push, then import the repository at vercel.com/new.
Vercel reads the same `vercel.json`, so the settings match either way. Add a custom domain under the
project's Domains tab.

### Anywhere else

Netlify, Cloudflare Pages, S3, GitHub Pages, or a directory on your own server. Copy
`public/index.html` in and you are done. Netlify and Cloudflare accept a drag and drop of the file
itself.

To embed it in a portfolio you already own, an `<iframe>` isolates the styling completely, since the
page has no globals beyond `NWLB` and makes no network calls.

```html
<iframe src="/nw-lb-go-figures.html" style="width:100%;height:900px;border:0" loading="lazy"
        title="nw-lb-go interactive figures"></iframe>
```

## Notes for a reader

The page is light only, on purpose. It is a technical drawing on paper, and the ink weights are
tuned for that ground. It paints its own background and declares `color-scheme: light`, so it keeps
its appearance inside a dark portfolio rather than borrowing one. It respects
`prefers-reduced-motion` by slowing packet spawning rather than freezing, so the controls still
demonstrate the behaviour.

Figures are only animated while scrolled into view, so the page costs about as much as the one
figure you are looking at, regardless of how many exist.
