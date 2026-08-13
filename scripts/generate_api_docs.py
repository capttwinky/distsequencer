from __future__ import annotations

import importlib
import inspect
import pkgutil
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PACKAGE = "distributed_sequencer"
OUTPUT = ROOT / "docs" / "api-reference.md"


def main() -> None:
    sys.path.insert(0, str(SRC))
    modules = tuple(_iter_modules(PACKAGE))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(_render_reference(modules), encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")


def _iter_modules(package_name: str) -> Iterable[ModuleType]:
    package = importlib.import_module(package_name)
    yield package
    package_paths = getattr(package, "__path__", None)
    if package_paths is None:
        return
    for module_info in pkgutil.walk_packages(package_paths, prefix=f"{package_name}."):
        if module_info.ispkg:
            continue
        yield importlib.import_module(module_info.name)


def _render_reference(modules: Iterable[ModuleType]) -> str:
    lines = [
        "# API Reference",
        "",
        "Generated from source docstrings with `make docs-api`.",
        "",
    ]
    for module in sorted(modules, key=lambda item: item.__name__):
        rendered = _render_module(module)
        if rendered:
            lines.extend(rendered)
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_module(module: ModuleType) -> list[str]:
    classes = tuple(_public_members(module, inspect.isclass))
    functions = tuple(_public_members(module, inspect.isfunction))
    if not classes and not functions:
        return []

    lines = [f"## `{module.__name__}`", ""]
    module_doc = inspect.getdoc(module)
    if module_doc:
        lines.extend((_escape_backticks(module_doc), ""))

    for name, value in classes:
        if value.__module__ == module.__name__:
            lines.extend(_render_object("class", name, value))
    for name, value in functions:
        if value.__module__ == module.__name__:
            lines.extend(_render_object("def", name, value))
    return lines


def _public_members(
    module: ModuleType,
    predicate: Callable[[Any], bool],
) -> Iterable[tuple[str, object]]:
    for name, value in inspect.getmembers(module, predicate):
        if name.startswith("_"):
            continue
        yield name, value


def _render_object(kind: str, name: str, value: object) -> list[str]:
    signature = _signature(value)
    lines = [f"### `{kind} {name}{signature}`", ""]
    doc = _authored_docstring(name, value)
    if doc:
        lines.extend((_escape_backticks(doc), ""))
    else:
        lines.extend(("_No docstring._", ""))
    return lines


def _signature(value: object) -> str:
    try:
        return str(inspect.signature(value))
    except (TypeError, ValueError):
        return ""


def _authored_docstring(name: str, value: object) -> str:
    raw = getattr(value, "__doc__", None)
    if not isinstance(raw, str):
        return ""
    doc = inspect.cleandoc(raw)
    if doc.startswith(f"{name}("):
        return ""
    if doc == "Unspecified run-time error.":
        return ""
    return doc


def _escape_backticks(value: str) -> str:
    return value.replace("```", "`\u200b``")


if __name__ == "__main__":
    main()
