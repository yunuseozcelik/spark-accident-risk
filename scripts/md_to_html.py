"""Markdown raporları yazdırılabilir HTML'e çevirir (tarayıcıdan Yazdır → PDF).

Kullanım:
    .venv/bin/python scripts/md_to_html.py docs/status_report_week7.md report/paper_draft.md
Çıktı, kaynak dosyanın yanına .html uzantısıyla yazılır.
"""
import sys
from pathlib import Path

import markdown

CSS = """
@page { size: A4; margin: 2cm; }
body { font-family: Georgia, 'DejaVu Serif', serif; font-size: 11pt;
       line-height: 1.45; max-width: 17cm; margin: 1.5cm auto; color: #111; }
h1 { font-size: 16pt; line-height: 1.25; }
h2 { font-size: 13pt; margin-top: 1.4em; border-bottom: 1px solid #999;
     padding-bottom: 2px; }
h3 { font-size: 11.5pt; }
table { border-collapse: collapse; width: 100%; font-size: 10pt; margin: 0.8em 0; }
th, td { border: 1px solid #888; padding: 4px 8px; text-align: left; }
th { background: #eee; }
code { font-family: 'DejaVu Sans Mono', monospace; font-size: 9.5pt;
       background: #f2f2f2; padding: 0 3px; }
blockquote { border-left: 3px solid #999; margin-left: 0; padding-left: 1em;
             color: #333; font-style: italic; }
@media print { body { margin: 0; } }
"""

TEMPLATE = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
{body}
</body>
</html>
"""


def convert(md_path: Path) -> Path:
    text = md_path.read_text(encoding="utf-8")
    body = markdown.markdown(text, extensions=["tables", "smarty"])
    title = next(
        (l.lstrip("# ").strip() for l in text.splitlines() if l.startswith("# ")),
        md_path.stem,
    )
    out = md_path.with_suffix(".html")
    out.write_text(TEMPLATE.format(title=title, css=CSS, body=body), encoding="utf-8")
    return out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for arg in sys.argv[1:]:
        print(f"Yazıldı: {convert(Path(arg))}")
