#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

TAIL_LINES = 200
MAX_LINE_LEN = 2000

ERROR_PATTERNS = re.compile(
    r"(ERROR|Exception|timeout|timed out|refused|reset|broken pipe|ssh|failed|failure|crash)",
    re.IGNORECASE,
)

ANALYSIS_INVALID_PAT = re.compile(r"analysis invalid", re.IGNORECASE)
ANALYSIS_ERRORS_NO_ANOMALIES_PAT = re.compile(
    r"errors occurred during analysis.*no anomalies found", re.IGNORECASE
)

MODELS_API_URL = "https://models.github.ai/inference/chat/completions"
MODEL_ID = "openai/gpt-4.1-mini"


def load_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return [line.rstrip("\n") for line in f]


def shorten(line: str, max_len: int = MAX_LINE_LEN) -> str:
    return line[:max_len]


def last_nonempty_line(lines: list[str]) -> str:
    for line in reversed(lines):
        if line.strip():
            return line.strip()
    return ""


def extract_tail(lines: list[str], n: int = TAIL_LINES) -> list[str]:
    return [shorten(x) for x in lines[-n:]]


def extract_error_snippets(lines: list[str], limit: int = 120) -> list[str]:
    out = []
    for line in lines:
        if ERROR_PATTERNS.search(line):
            out.append(shorten(line))
    return out[-limit:]


def categorize_final_line(final_line: str) -> str:
    if ANALYSIS_INVALID_PAT.search(final_line):
        return "ANALYSIS_INVALID"
    if ANALYSIS_ERRORS_NO_ANOMALIES_PAT.search(final_line):
        return "ANALYSIS_ERRORS_NO_ANOMALIES"
    return "OTHER_FAILURE"


def parse_json_content(content: str) -> dict:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
    return json.loads(text)


def call_github_models(
    final_line: str,
    category: str,
    tail_lines: list[str],
    error_lines: list[str],
) -> dict:
    token = os.environ["GITHUB_TOKEN"]

    user_prompt = f"""
You are analyzing a failed Jepsen test.

The test did NOT succeed.

The final status line is:

{final_line}

This final status line has been categorized as: {category}

Category meanings:
- ANALYSIS_INVALID: "Analysis invalid!" — the checker/analyzer reported the run
  as invalid, but this does NOT by itself prove a real consistency bug.
- ANALYSIS_ERRORS_NO_ANOMALIES: "Errors occurred during analysis, but no
  anomalies found." — suggests analysis issues or infrastructure problems
  without confirmed anomalies.
- OTHER_FAILURE: anything else; typically an infrastructure or runtime error
  before analysis could finish.

These signals must be interpreted together with the logs.

Your task:
Classify this failure into exactly one of:

- TEMPORARY_ISSUE
  (likely transient, environmental, or infrastructure-related)

- INCONSISTENCY_REQUIRES_INVESTIGATION
  (credible evidence of a real consistency or correctness violation)

- UNKNOWN_REQUIRES_HUMAN
  (insufficient or conflicting evidence; needs manual inspection)

Be conservative:
- Only choose INCONSISTENCY_REQUIRES_INVESTIGATION if there is clear evidence
  of a real anomaly (e.g., inconsistency, stale read, lost update, contradiction).
- "Analysis invalid!" alone is NOT sufficient evidence.
- Prefer UNKNOWN_REQUIRES_HUMAN over making a weak or speculative conclusion.
- Choose TEMPORARY_ISSUE only if the failure is well-explained by
  transient or environmental issues.

Return strict JSON only with this schema:

{{
  "label": "TEMPORARY_ISSUE" | "INCONSISTENCY_REQUIRES_INVESTIGATION" | "UNKNOWN_REQUIRES_HUMAN",
  "confidence": 0.0,
  "reasoning": "short explanation",
  "evidence": ["key observation", "..."]
}}

---

FINAL_LINE:
{final_line}

---

TAIL_LOG (last {TAIL_LINES} lines):
{chr(10).join(tail_lines)}

---

ERROR_SNIPPETS:
{chr(10).join(error_lines)}
""".strip()

    body = {
        "model": MODEL_ID,
        "messages": [
            {
                "role": "system",
                "content": "You are a careful CI failure triage assistant. Output JSON only."
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    req = Request(
        MODELS_API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(req) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub Models API error: {e.code} {detail}") from e

    content = payload["choices"][0]["message"]["content"]
    return parse_json_content(content)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    log_path = Path(args.log)
    summary_path = Path(args.summary)

    lines = load_lines(log_path)
    final_line = last_nonempty_line(lines)
    category = categorize_final_line(final_line)

    tail = extract_tail(lines, TAIL_LINES)
    errors = extract_error_snippets(lines)
    result = call_github_models(final_line, category, tail, errors)

    md = []
    md.append("## Jepsen triage")
    md.append("")
    md.append(f"- label: **{result['label']}**")
    md.append(f"- confidence: **{result['confidence']}**")
    md.append("")
    md.append("### reasoning")
    md.append(result["reasoning"])
    md.append("")
    md.append("### evidence")
    for e in result["evidence"]:
        md.append(f"- {e}")
    md.append("")
    md.append("### final line")
    md.append("```")
    md.append(final_line)
    md.append("```")

    summary_path.write_text("\n".join(md), encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"triage failed: {e}", file=sys.stderr)
        raise
