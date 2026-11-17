#!/usr/bin/env python3
# =============================================================================
# © 2026 Satyendra Singh. All Rights Reserved.
# PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
# =============================================================================
"""Builds the NexusSaga documentation PDFs from the Markdown chapters in docs/.

Usage:  python3 docs/build_pdfs.py [--chrome /path/to/chrome]
Needs:  pip install markdown playwright   (Chrome or Chromium for printing)
"""
import argparse
import asyncio
import html
import pathlib
import re

import markdown
from playwright.async_api import async_playwright

DOCS = pathlib.Path(__file__).resolve().parent
OUT = DOCS / "pdf"
BUILD = DOCS / ".build"
DATE = "16 November 2026"
VERSION = "1.0"
CHAPTERS = sorted(DOCS.glob("0[1-9]-*.md"))

CSS = """
@page { size: A4; margin: 20mm 17mm 20mm 17mm; }
* { box-sizing: border-box; }
body { font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color: #1e293b; font-size: 10.3pt; line-height: 1.55; margin: 0; }
h1 { font-size: 19pt; color: #1e1b4b; border-bottom: 2.5px solid #4f46e5; padding-bottom: 5px; margin: 0 0 12px; break-before: page; }
h1.first { break-before: auto; }
h2 { font-size: 13.5pt; color: #312e81; margin: 20px 0 8px; break-after: avoid; }
h3 { font-size: 11.5pt; color: #3730a3; margin: 14px 0 6px; break-after: avoid; }
p { margin: 0 0 9px; }
a { color: #4f46e5; text-decoration: none; }
ul, ol { margin: 0 0 10px; padding-left: 20px; } li { margin-bottom: 3px; }
code { font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 8.8pt; background: #f1f5f9; color: #0f172a; padding: 1px 4px; border-radius: 3px; }
pre { background: #0f172a; color: #e2e8f0; padding: 11px 13px; border-radius: 7px; font-size: 8.4pt; line-height: 1.45; white-space: pre-wrap; word-break: break-word; break-inside: avoid; margin: 0 0 12px; }
pre code { background: none; color: inherit; padding: 0; font-size: inherit; }
table { width: 100%; border-collapse: collapse; margin: 6px 0 14px; font-size: 9pt; break-inside: auto; }
thead { display: table-header-group; }
tr { break-inside: avoid; }
th { background: #eef2ff; color: #1e1b4b; text-align: left; font-weight: 600; padding: 6px 8px; border: 1px solid #c7d2fe; }
td { padding: 5px 8px; border: 1px solid #e2e8f0; vertical-align: top; }
tr:nth-child(even) td { background: #f8fafc; }
blockquote { margin: 0 0 12px; padding: 8px 12px; background: #fffbeb; border-left: 4px solid #d97706; color: #78350f; border-radius: 0 6px 6px 0; }
blockquote p { margin: 0; }
figure { margin: 6px 0 16px; text-align: center; break-inside: avoid; }
figure img { max-width: 100%; max-height: 215mm; object-fit: contain; border: 1px solid #e2e8f0; border-radius: 6px; padding: 6px; background: #fff; }
figcaption { font-size: 8.6pt; color: #64748b; margin-top: 4px; font-style: italic; }
.cover { min-height: 250mm; display: flex; flex-direction: column; break-after: page; }
.band { background: linear-gradient(135deg, #312e81 0%, #4f46e5 55%, #a21caf 100%); color: #fff; border-radius: 14px; padding: 34px 32px 30px; margin-top: 24mm; }
.logo { width: 46px; height: 46px; border-radius: 11px; background: rgba(255,255,255,.18); display: flex; align-items: center; justify-content: center; font-weight: 800; font-size: 22px; margin-bottom: 26px; }
.band .kicker { font-size: 10pt; letter-spacing: .14em; text-transform: uppercase; opacity: .8; }
.band .title { font-size: 30pt; font-weight: 800; line-height: 1.12; margin: 8px 0 10px; }
.band .subtitle { font-size: 12.5pt; opacity: .92; max-width: 140mm; }
.meta { margin-top: 18mm; }
.meta table td { border: none; border-bottom: 1px solid #e2e8f0; background: none !important; padding: 7px 4px; font-size: 10pt; }
.meta table td:first-child { color: #64748b; width: 45mm; }
.cover .foot { margin-top: auto; font-size: 8.5pt; color: #94a3b8; border-top: 1px solid #e2e8f0; padding-top: 8px; }
.toc { break-after: page; }
.toc h1 { break-before: auto; }
.toc ol { list-style: none; padding: 0; }
.toc li { padding: 4px 0; border-bottom: 1px dotted #cbd5e1; }
.toc li.l2 { padding-left: 16px; font-size: 9.3pt; color: #475569; }
.toc li.l1 { font-weight: 600; margin-top: 4px; }
.chapter-cover { break-before: page; break-after: page; height: 240mm; display: flex; flex-direction: column; justify-content: center; }
.chapter-cover .no { font-size: 11pt; color: #6366f1; letter-spacing: .15em; text-transform: uppercase; font-weight: 700; }
.chapter-cover .t { font-size: 28pt; font-weight: 800; color: #1e1b4b; margin: 6px 0; }
.chapter-cover .s { font-size: 12pt; color: #475569; max-width: 140mm; }
.chapter-cover .bar { width: 60mm; height: 5px; background: linear-gradient(90deg, #4f46e5, #a21caf); border-radius: 3px; margin-top: 14px; }
"""

FOOTER = """<div style="font-family:Helvetica,Arial;font-size:7.5pt;color:#94a3b8;width:100%;padding:0 17mm;display:flex;justify-content:space-between">
<span>NexusSaga · {title}</span><span>© 2026 Satyendra Singh · Page <span class="pageNumber"></span> / <span class="totalPages"></span></span></div>"""
HEADER = """<div style="font-family:Helvetica,Arial;font-size:7.5pt;color:#cbd5e1;width:100%;padding:0 17mm;text-align:right">{doc} · v{version}</div>"""


def parse(path):
    text = path.read_text(encoding="utf-8")
    meta = {}
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    if m:
        for line in m.group(1).splitlines():
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()
        text = text[m.end():]
    return meta, text


def to_html(md_text, prefix):
    body = markdown.markdown(md_text, extensions=["tables", "fenced_code", "sane_lists"])
    # Images in their own paragraph become captioned figures.
    body = re.sub(r'<p><img alt="([^"]*)" src="([^"]+)"\s*/?></p>',
                  r'<figure><img src="\2" alt="\1"/><figcaption>Figure: \1</figcaption></figure>', body)
    headings = []

    def anchor(match):
        level, title = match.group(1), match.group(2)
        hid = f"{prefix}-{len(headings)}"
        headings.append((int(level), re.sub("<[^>]+>", "", title), hid))
        return f'<h{level} id="{hid}">{title}</h{level}>'

    body = re.sub(r"<h([12])>(.*?)</h\1>", anchor, body)
    body = body.replace("<h1 ", '<h1 class="first" ', 1)
    return body, headings


def cover(title, subtitle, doc_id, kicker="NexusSaga · Technical Documentation"):
    rows = [("Document", doc_id), ("Version", VERSION), ("Date", DATE),
            ("Author", "Satyendra Singh (Senior Java Backend Developer)"),
            ("Product", "NexusSaga – Enterprise Generic Saga Orchestration Engine"),
            ("Stack", "Java 21 · Spring Boot 3.4 · PostgreSQL 16 · Apache Kafka · Hazelcast")]
    meta = "".join(f"<tr><td>{k}</td><td>{html.escape(v)}</td></tr>" for k, v in rows)
    return f"""<section class="cover"><div class="band"><div class="logo">N</div>
<div class="kicker">{kicker}</div><div class="title">{html.escape(title)}</div>
<div class="subtitle">{html.escape(subtitle)}</div></div>
<div class="meta"><table>{meta}</table></div>
<div class="foot">© 2026 Satyendra Singh. All Rights Reserved. · Confidential – for internal and review use.</div></section>"""


def toc(entries, title="Contents"):
    items = "".join(f'<li class="l{lvl}"><a href="#{hid}">{html.escape(t)}</a></li>' for lvl, t, hid in entries)
    return f'<section class="toc"><h1>{title}</h1><ol>{items}</ol></section>'


def page(body):
    return f'<!DOCTYPE html><html><head><meta charset="utf-8"><style>{CSS}</style></head><body>{body}</body></html>'


async def render(browser, html_text, name, footer_title, doc_id):
    BUILD.mkdir(exist_ok=True)
    src = DOCS / f".{name}.html"          # lives in docs/ so relative image paths resolve
    src.write_text(html_text, encoding="utf-8")
    pg = await browser.new_page()
    await pg.goto(src.as_uri(), wait_until="networkidle")
    await pg.pdf(path=str(OUT / f"{name}.pdf"), format="A4", print_background=True,
                 display_header_footer=True,
                 header_template=HEADER.format(doc=doc_id, version=VERSION),
                 footer_template=FOOTER.format(title=html.escape(footer_title)),
                 margin={"top": "20mm", "bottom": "20mm", "left": "17mm", "right": "17mm"})
    await pg.close()
    src.unlink()
    print("built", OUT / f"{name}.pdf")


async def main(chrome):
    OUT.mkdir(exist_ok=True)
    chapters = []
    for index, path in enumerate(CHAPTERS, start=1):
        meta, text = parse(path)
        body, heads = to_html(text, f"c{index}")
        chapters.append((path, meta, body, heads))

    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=chrome) if chrome else await p.chromium.launch()
        for path, meta, body, heads in chapters:
            name = "NexusSaga_" + path.stem[3:].replace("-", "_").title().replace("And", "and").replace("Api_", "API_")
            html_text = page(cover(meta["title"], meta["subtitle"], meta["doc"]) + toc(heads) + body)
            await render(browser, html_text, f"{path.stem[:2]}_{name}", meta["title"], meta["doc"])

        # Combined handbook
        parts = [cover("Complete Technical Documentation",
                       "Architecture, saga process flows, security, API integration and operations "
                       "of the NexusSaga orchestration engine", "NXS-DOC-00 (Handbook)")]
        entries = []
        for i, (path, meta, body, heads) in enumerate(chapters, start=1):
            entries.append((1, f"Part {i} – {meta['title']}", f"part{i}"))
            entries.extend((2, t, hid) for lvl, t, hid in heads if lvl == 1)
        parts.append(toc(entries, "Handbook contents"))
        for i, (path, meta, body, heads) in enumerate(chapters, start=1):
            parts.append(f'<section class="chapter-cover" id="part{i}"><div class="no">Part {i} · {meta["doc"]}</div>'
                         f'<div class="t">{html.escape(meta["title"])}</div><div class="s">{html.escape(meta["subtitle"])}</div>'
                         f'<div class="bar"></div></section>')
            parts.append(body.replace('<h1 class="first" ', "<h1 ", 1))
        await render(browser, page("".join(parts)), "00_NexusSaga_Complete_Documentation",
                     "Complete Technical Documentation", "NXS-DOC-00")
        await browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chrome", help="path to a Chrome/Chromium executable")
    asyncio.run(main(parser.parse_args().chrome))
