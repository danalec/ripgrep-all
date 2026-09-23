#!/usr/bin/env python3
"""Generate the rga benchmark corpus deterministically.

The corpus is NOT checked into git (benches/corpus/ is gitignored); CI and
local runs generate it fresh. Determinism goals: fixed timestamps and metadata
on every container (zip/tar/docx/pdf), seeded RNG for all text. Regenerating
yields a corpus with identical sizes and character; container byte-level hashes
are informational only (sqlite headers and pdf ids can drift across library
versions).

Usage: python benches/gen_corpus.py
Requires: reportlab, python-docx (both bundled with the Kimi Work python).
"""
import hashlib
import io
import json
import random
import sqlite3
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).parent / "corpus"
WORDS = (
    "the quick brown fox jumps over lazy dog while searching through archives "
    "of pdf documents and office files packed deep inside compressed folders "
    "for a needle in a haystack ripgrep all adapters preprocessing cache"
).split()


def seeded_text(rng: random.Random, words: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(words))


def write_text_files() -> None:
    rng = random.Random(20240922)

    def lines(n: int, words_per_line: int = 14) -> str:
        return "".join(seeded_text(rng, words_per_line) + "\n" for _ in range(n))

    # plain text, 1 MiB of short lines (search-friendly)
    (ROOT / "plain_ascii_1m.txt").write_text(lines(52_000), encoding="utf-8")
    # larger plain text, 16 MiB
    (ROOT / "plain_ascii_16m.txt").write_text(lines(830_000), encoding="utf-8")
    # markdown-ish document, 4 MiB
    md = "".join(
        f"## section {i}\n\n{seeded_text(rng, 120)}.\n\n" for i in range(4_500)
    )
    (ROOT / "markdown_doc.md").write_text(md, encoding="utf-8")


def write_pdf() -> None:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.pdfgen import canvas

    rng = random.Random(20240923)
    path = ROOT / "pdf_text.pdf"
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    c.setTitle("rga bench corpus")
    c.setAuthor("rga bench")
    c.setCreator("rga bench")
    c.setSubject("deterministic benchmark corpus")
    # fixed document date so regenerating keeps stable metadata
    c._doc.info.date = "D:20240101000000+00'00'"
    width, height = LETTER
    for page in range(1200):
        c.setFont("Helvetica", 10)
        y = height - 72
        for _ in range(54):
            c.drawString(72, y, seeded_text(rng, 16)[:95])
            y -= 13
        c.showPage()
    c.save()
    path.write_bytes(buf.getvalue())


def write_docx() -> None:
    import docx

    rng = random.Random(20240924)
    path = ROOT / "office.docx"
    d = docx.Document()
    props = d.core_properties
    props.author = "rga bench"
    props.title = "rga bench corpus"
    props.created = props.modified = props.last_printed = __import__(
        "datetime"
    ).datetime(2024, 1, 1, tzinfo=__import__("datetime").timezone.utc)
    props.revision = 1
    for i in range(15_000):
        p = d.add_paragraph(f"Heading {i}: " + seeded_text(rng, 40))
        if i % 10 == 0:
            p.style = d.styles["Heading 2"]
    d.save(str(path))


def write_archives() -> None:
    rng = random.Random(20240925)

    def member_bytes(n: int) -> bytes:
        return seeded_text(rng, n).encode()

    # flat zip: 100 text files
    with zipfile.ZipFile(ROOT / "archive_flat.zip", "w") as z:
        for i in range(100):
            zi = zipfile.ZipInfo(f"doc_{i:03d}.txt", date_time=(2024, 1, 1, 0, 0, 0))
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = 0o644 << 16
            z.writestr(zi, member_bytes(4_000))

    # nested zip (zip in zip), what stresses the recursion path
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, "w") as z:
        for i in range(40):
            zi = zipfile.ZipInfo(f"inner_{i:03d}.txt", date_time=(2024, 1, 1, 0, 0, 0))
            zi.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(zi, member_bytes(2_000))
    with zipfile.ZipFile(ROOT / "archive_nested.zip", "w") as z:
        for i in range(12):
            zi = zipfile.ZipInfo(f"level1_{i:02d}.zip", date_time=(2024, 1, 1, 0, 0, 0))
            zi.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(zi, inner_buf.getvalue())

    # tar.gz with mixed members
    with tarfile.open(ROOT / "archive_mixed.tar.gz", "w:gz") as t:
        for i in range(120):
            data = member_bytes(3_000)
            ti = tarfile.TarInfo(f"folder/sub/file_{i:03d}.txt")
            ti.size = len(data)
            ti.mtime = 1_704_067_200  # 2024-01-01
            ti.uid = ti.gid = 0
            ti.uname = ti.gname = "root"
            ti.mode = 0o644
            t.addfile(ti, io.BytesIO(data))


def write_sqlite() -> None:
    rng = random.Random(20240926)
    path = ROOT / "db.sqlite"
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
    con.executemany(
        "INSERT INTO docs (id, body) VALUES (?, ?)",
        ((i, seeded_text(rng, 900)) for i in range(5_000)),
    )
    con.commit()
    con.execute("PRAGMA journal_mode=DELETE")
    con.execute("VACUUM")
    con.close()


def write_manifest() -> None:
    entries = {}
    for f in sorted(ROOT.iterdir()):
        if f.name == "MANIFEST.json":
            continue
        data = f.read_bytes()
        entries[f.name] = {
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    (ROOT / "MANIFEST.json").write_text(
        json.dumps(entries, indent=2) + "\n", encoding="utf-8"
    )


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    write_text_files()
    write_pdf()
    write_docx()
    write_archives()
    write_sqlite()
    write_manifest()
    total = sum(f.stat().st_size for f in ROOT.iterdir() if f.name != "MANIFEST.json")
    print(f"corpus written to {ROOT} ({total / (1 << 20):.1f} MiB)")


if __name__ == "__main__":
    main()
