#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schema" / "nexuskv_contract_v1.json"
PYTHON_OUT = ROOT / "python" / "nexuskv" / "contracts" / "generated.py"
RUST_OUT = ROOT / "rust" / "crates" / "nexus-state" / "src" / "generated.rs"


def snake_wire(name: str) -> str:
    return name.lower()


def pascal(name: str) -> str:
    return "".join(part.capitalize() for part in name.lower().split("_"))


def py_type(type_name: str) -> str:
    if type_name == "string":
        return "str"
    if type_name == "int":
        return "int"
    if type_name == "bool":
        return "bool"
    if type_name.startswith("optional[") and type_name.endswith("]"):
        return f"{py_type(type_name[9:-1])} | None"
    if type_name.startswith("list[") and type_name.endswith("]"):
        return f"list[{py_type(type_name[5:-1])}]"
    if type_name == "map[string,string]":
        return "dict[str, str]"
    return type_name


def rust_type(type_name: str) -> str:
    if type_name == "string":
        return "String"
    if type_name == "int":
        return "u32"
    if type_name == "bool":
        return "bool"
    if type_name.startswith("optional[") and type_name.endswith("]"):
        return f"Option<{rust_type(type_name[9:-1])}>"
    if type_name.startswith("list[") and type_name.endswith("]"):
        return f"Vec<{rust_type(type_name[5:-1])}>"
    if type_name == "map[string,string]":
        return "BTreeMap<String, String>"
    return type_name


def generate_python(schema: dict) -> str:
    lines: list[str] = [
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
        "from enum import StrEnum",
        "",
        f'SCHEMA_VERSION = "{schema["schema_version"]}"',
        "",
    ]

    for enum_name, values in schema["enums"].items():
        lines.append(f"class {enum_name}(StrEnum):")
        for value in values:
            lines.append(f'    {value} = "{snake_wire(value)}"')
        lines.append("")

    for object_name, spec in schema["objects"].items():
        lines.append("@dataclass(slots=True)")
        lines.append(f"class {object_name}:")
        for field in spec["fields"]:
            lines.append(f'    {field["name"]}: {py_type(field["type"])}')
        lines.append("")

    exported = ["SCHEMA_VERSION", *schema["enums"].keys(), *schema["objects"].keys()]
    lines.append("__all__ = [")
    for name in exported:
        lines.append(f'    "{name}",')
    lines.append("]")
    lines.append("")
    return "\n".join(lines)


def generate_rust(schema: dict) -> str:
    lines: list[str] = [
        "use serde::{Deserialize, Serialize};",
        "use std::collections::BTreeMap;",
        "",
        f'pub const SCHEMA_VERSION: &str = "{schema["schema_version"]}";',
        "",
    ]

    for enum_name, values in schema["enums"].items():
        lines.extend(
            [
                '#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]',
                '#[serde(rename_all = "snake_case")]',
                f"pub enum {enum_name} {{",
            ]
        )
        for value in values:
            lines.append(f"    {pascal(value)},")
        lines.append("}")
        lines.append("")
    for object_name, spec in schema["objects"].items():
        lines.extend(
            [
                "#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]",
                f"pub struct {object_name} {{",
            ]
        )
        for field in spec["fields"]:
            lines.append(f'    pub {field["name"]}: {rust_type(field["type"])},')
        lines.append("}")
        lines.append("")
    return "\n".join(lines)


def write_or_check(path: Path, content: str, check: bool) -> bool:
    if check:
        return path.exists() and path.read_text() == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    schema = json.loads(SCHEMA_PATH.read_text())
    python_content = generate_python(schema)
    rust_content = generate_rust(schema)

    ok = True
    ok &= write_or_check(PYTHON_OUT, python_content, args.check)
    ok &= write_or_check(RUST_OUT, rust_content, args.check)

    if args.check and not ok:
        print("generated contracts are out of date")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
