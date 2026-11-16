"""Draw a website SVG logo candidate once, only so LegBot's `judge_logo` can look at it (SYNC-163).

The drawing is never stored or sent to the broker: when the candidate is accepted, the broker receives the ORIGINAL SVG and
cleans it itself (BROKER-503); the website draws it with `<img>` (NEXT-216).

The renderer is resvg (`resvg-py`): no scripting and no network (checked 2026-11-16: an `http://` image and a CSS `@import`
made no request). It DOES read a local file named by a plain path (`<image href="/some/file.png">`, or a relative one), so
`for_drawing` removes every reference that is not a `#fragment`, and every link on an `<image>` (resvg reads even `#name`
there as a file path), before anything is drawn. Embedded `data:` pictures go too (SYNC-168): **the judge must see exactly
what the broker will store**, and the broker's cleaner (`content.svg.clean`, BROKER-503) keeps only `#fragment` links, so an
SVG that only wraps a picture draws nothing and is not judged (`unreadable`). The drawing runs in its own process, in an
empty temporary folder, with a time limit (killed on a timeout or when the caller is cancelled) and a
fixed output size (`DRAWN_LONGEST_SIDE` on the longest side, so a tiny viewBox is not dropped by the 128 px rule and a huge
one cannot make a huge picture). The parser is the standard library's expat with no entities: a file that declares an entity
or has an internal DOCTYPE subset is refused. A file that cannot be read or drawn raises SVGUnreadable; the caller counts it
as `unreadable`. System fonts are loaded, because a wordmark drawn without them is blank and the judge needs the name.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
from xml.parsers import expat

SVG_NS = "http://www.w3.org/2000/svg"
DRAWN_LONGEST_SIDE = 512
DRAW_TIMEOUT_SECONDS = 20.0
MAX_ELEMENTS = 20_000
MAX_DEPTH = 100  # nesting; deeper is not a logo (and would exhaust the recursion below)
XML_BASE = "{http://www.w3.org/XML/1998/namespace}base"
BACKGROUND = "#808080"  # the same mid-grey the raster candidates are shown on (organization_logo.GREY)
_URL = re.compile(r"url\(\s*(['\"]?)(.*?)\1\s*\)", re.I | re.S)
_IMPORT = re.compile(r"@import[^;]*;?", re.I)

ET.register_namespace("", SVG_NS)
ET.register_namespace("xlink", "http://www.w3.org/1999/xlink")


class SVGUnreadable(Exception):
    """The SVG cannot be read or drawn safely; counted as `unreadable`, never raised past the logo step."""


def renderer_installed() -> bool:
    """Whether `resvg-py` is installed here. Until it is (a release step), website SVGs keep being skipped as `svg`."""
    return importlib.util.find_spec("resvg_py") is not None


def _parse(data: bytes) -> ET.Element:
    builder = ET.TreeBuilder()
    parser = expat.ParserCreate(namespace_separator="}")
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    count, depth = [0], [0]

    def refuse(*_args):
        raise SVGUnreadable("the SVG declares entities or an internal document type")

    def doctype(_name, _system_id, _public_id, has_internal_subset):
        if has_internal_subset:
            refuse()

    def name(raw):
        return "{" + raw if "}" in raw else raw

    def start(tag, attrs):
        count[0] += 1
        depth[0] += 1
        if count[0] > MAX_ELEMENTS or depth[0] > MAX_DEPTH:
            raise SVGUnreadable("too many elements, or nested too deeply")
        builder.start(name(tag), {name(k): v for k, v in attrs.items()})

    def end(tag):
        depth[0] -= 1
        builder.end(name(tag))

    parser.StartDoctypeDeclHandler = doctype
    parser.EntityDeclHandler = refuse
    parser.UnparsedEntityDeclHandler = refuse
    parser.ExternalEntityRefHandler = refuse
    parser.SkippedEntityHandler = refuse
    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = builder.data
    try:
        parser.Parse(data, True)
        root = builder.close()
    except expat.ExpatError as exc:
        raise SVGUnreadable(f"not readable XML ({expat.ErrorString(exc.code)})") from None
    if root.tag != f"{{{SVG_NS}}}svg":
        raise SVGUnreadable("not an SVG")
    return root


def _risky_css(text: str) -> bool:
    """An escape, a comment or an at-rule can hide an address from the patterns below: such a style is dropped whole."""
    return "\\" in text or "/*" in text or "@" in text


def _clean_css(text: str) -> str:
    text = _IMPORT.sub("", text)
    return _URL.sub(lambda m: m.group(0) if m.group(2).strip().startswith("#") else "none", text)


def _strip_references(element: ET.Element) -> None:
    for child in element:
        _strip_references(child)
    for key in list(element.attrib):
        local, value = key.rsplit("}", 1)[-1].lower(), element.attrib[key]
        # xml:base re-points a "#fragment"; a backslash (escape) or a comment can hide an address from the patterns below
        if key == XML_BASE or "\\" in value or "/*" in value:
            del element.attrib[key]
        elif local == "href":
            # SYNC-168: the judge must see what the broker will store, and the broker's cleaner (content.svg.clean,
            # BROKER-503) keeps only "#fragment" links: an embedded data: picture goes too (Dow Chemical's logo, an SVG
            # wrapping a PNG, was judged on the PNG and stored blank). SYNC-169: on <image> even a "#" link goes, since
            # resvg reads it as a file path.
            if not value.strip().startswith("#") or element.tag.rsplit("}", 1)[-1] == "image":
                del element.attrib[key]
        elif local == "style" and _risky_css(value):
            del element.attrib[key]
        elif local == "style" or "url(" in value.lower():
            element.attrib[key] = _clean_css(value)
    if element.tag.rsplit("}", 1)[-1] == "style":
        text = "".join(element.itertext())  # including text after a child element, which ElementTree keeps on the child
        for child in list(element):
            element.remove(child)
        element.text = "" if _risky_css(text) else _clean_css(text)


def _size(root: ET.Element) -> tuple[float, float] | None:
    box = (root.get("viewBox") or "").replace(",", " ").split()
    try:
        if len(box) == 4 and float(box[2]) > 0 and float(box[3]) > 0:
            return float(box[2]), float(box[3])
        w, h = (float(re.sub(r"px\s*$", "", root.get(a) or "", flags=re.I)) for a in ("width", "height"))
        return (w, h) if w > 0 and h > 0 else None
    except ValueError:
        return None


def for_drawing(data: bytes) -> tuple[bytes, dict]:
    """The SVG with every outside reference removed, and the size to draw it at ({"width": 512} or {"height": 512}, by its
    shape; square when it gives no size). Raises SVGUnreadable."""
    root = _parse(data)
    _strip_references(root)
    size = _size(root)
    side = {"height": DRAWN_LONGEST_SIDE} if size and size[1] > size[0] else {"width": DRAWN_LONGEST_SIDE}
    return ET.tostring(root, encoding="utf-8"), side


async def draw_for_judge(data: bytes, timeout: float = DRAW_TIMEOUT_SECONDS) -> bytes:
    """The SVG drawn as a PNG on the mid-grey backdrop, at most DRAWN_LONGEST_SIDE on its longest side, in a separate
    process that is killed after `timeout` seconds. Raises SVGUnreadable."""
    safe, side = for_drawing(data)
    (dimension, pixels), = side.items()
    with tempfile.TemporaryDirectory(prefix="svg-draw-") as empty:  # nothing to find by a relative path
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-I", __file__, dimension, str(pixels), empty,  # isolated: only this file, the stdlib and resvg_py
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=empty,
            env=_child_env(),
        )
        try:
            png, err = await asyncio.wait_for(process.communicate(safe), timeout)
        except TimeoutError:
            raise SVGUnreadable(f"drawing took longer than {timeout:g} s") from None
        finally:
            if process.returncode is None:  # timed out, or the caller was cancelled (SYNC-169): never leave the child running
                with contextlib.suppress(ProcessLookupError):  # it may have exited just now: keep the original error
                    process.kill()
                await process.wait()
    if process.returncode != 0 or not png.startswith(b"\x89PNG\r\n\x1a\n") or len(png) < 24:
        raise SVGUnreadable(f"could not be drawn: {err.decode('utf-8', 'replace').strip()[-200:]}")
    width, height = int.from_bytes(png[16:20], "big"), int.from_bytes(png[20:24], "big")
    if not (0 < width <= DRAWN_LONGEST_SIDE and 0 < height <= DRAWN_LONGEST_SIDE):
        raise SVGUnreadable(f"drawn at {width}x{height}, over {DRAWN_LONGEST_SIDE}")
    if _draws_nothing(png):
        raise SVGUnreadable("draws nothing (only the backdrop), for example an SVG that only wraps an embedded picture")
    return png


def _draws_nothing(png: bytes) -> bool:
    """Whether the drawing is the plain backdrop and nothing else (SYNC-168): it is not judged at all."""
    import io

    from PIL import Image

    grey = tuple(int(BACKGROUND[i:i + 2], 16) for i in (1, 3, 5))
    with Image.open(io.BytesIO(png)) as img:
        return all(low == high == level for (low, high), level in zip(img.convert("RGB").getextrema(), grey))


def _child_env() -> dict:
    """A minimal environment for the child (`-I` already ignores PYTHON* variables and the user's site packages)."""
    import os

    return {key: os.environ[key] for key in ("PATH", "HOME", "LANG") if key in os.environ}


def _main(argv: list[str]) -> int:
    """Child process: read the (already reference-free) SVG on stdin and write the PNG on stdout."""
    import resvg_py

    dimension, pixels, resources = argv[0], int(argv[1]), argv[2]
    svg = sys.stdin.buffer.read().decode("utf-8")
    png = resvg_py.svg_to_bytes(svg_string=svg, background=BACKGROUND, resources_dir=resources, **{dimension: pixels})
    sys.stdout.buffer.write(bytes(png))
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
