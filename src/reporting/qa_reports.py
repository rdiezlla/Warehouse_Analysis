from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_qa_report(qa_payload: dict, output_path: Path) -> None:
    lines = ["# QA Summary", ""]
    for section, payload in qa_payload.items():
        lines.append(f"## {section}")
        if isinstance(payload, dict):
            for key, value in payload.items():
                lines.append(f"- {key}: {value}")
        else:
            lines.append(f"- {payload}")
        lines.append("")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def save_join_reports(join_outputs: dict, output_dir: Path) -> None:
    for key in ["coverage_global", "coverage_by_service", "coverage_by_movement", "unmatched_top", "internal_movement_summary"]:
        value = join_outputs.get(key)
        if isinstance(value, pd.DataFrame):
            value.to_csv(output_dir / f"{key}.csv", index=False)
