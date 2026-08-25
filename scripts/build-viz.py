#!/usr/bin/env python3
"""Inline the visualisation sources into one self-contained page.

The output has no external requests of any kind, so it can be dropped onto any static host — or
opened straight off the filesystem — without a build step or a CDN.
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "docs" / "viz" / "src"
OUT = ROOT / "docs" / "viz" / "index.html"
ARTIFACT_OUT = ROOT / "docs" / "viz" / "artifact.html"

# Registration order is figure order on the page.
FIGURES = [
    "ring",
    "failover",
    "omission",
]


def read(path):
    if not path.exists():
        sys.exit(f"build-viz: missing {path.relative_to(ROOT)}")
    return path.read_text(encoding="utf-8")


def main():
    shell = read(SRC / "shell.html")
    css = read(SRC / "style.css")

    parts = [read(SRC / "core.js")]
    missing = []
    for name in FIGURES:
        p = SRC / "diagrams" / f"{name}.js"
        if not p.exists():
            missing.append(name)
            continue
        parts.append(f"/* ---- {name} ---- */\n" + p.read_text(encoding="utf-8"))
    if missing:
        print(f"build-viz: warning: no module for {', '.join(missing)}", file=sys.stderr)

    parts.append("NWLB.mountAll(document.getElementById('figures'));")
    js = "\n\n".join(parts)

    # str.replace, not a format string: the sources are full of braces and percent signs.
    page = shell.replace("/*__CSS__*/", css).replace("/*__JS__*/", js)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(page, encoding="utf-8")
    kb = len(page.encode("utf-8")) / 1024
    print(f"build-viz: wrote {OUT.relative_to(ROOT)} ({kb:.0f} KB, {len(FIGURES) - len(missing)} figures)")

    # Claude Artifacts supply their own <!doctype>/<html>/<head>/<body>, so that variant carries the
    # page's content only. Everything else — styles, script, theming — is identical.
    body = page.split("<body>", 1)[1].rsplit("</body>", 1)[0]
    artifact = (
        "<title>nw-lb-go</title>\n"
        + f"<style>{css}</style>\n"
        + body.strip()
        + "\n"
    )
    ARTIFACT_OUT.write_text(artifact, encoding="utf-8")
    akb = len(artifact.encode("utf-8")) / 1024
    print(f"build-viz: wrote {ARTIFACT_OUT.relative_to(ROOT)} ({akb:.0f} KB, artifact variant)")


if __name__ == "__main__":
    main()
