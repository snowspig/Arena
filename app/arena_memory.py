"""Arena agent memory palace utilities."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_MEMORY_ROOT = Path("memory/arena")
_MAX_LESSON_CHARS = 500
_DATE_SECTION_PATTERN = re.compile(r"^## \d{4}-\d{2}-\d{2}.*$", re.MULTILINE)
_MEMORY_FILENAMES = ("facts.md", "lessons.md", "preferences.md")
_AGENT_MODELS: dict[str, str] = {
    "vllm_trader_pro": "Qwopus3.5-27B-v3",
    "nadirclaw_claude_sonnet": "claude-sonnet-4-6",
    "nadirclaw_claude_opus": "claude-opus-4-6",
    "nadirclaw_gpt": "gpt-5.4",
    "nadirclaw_glm5_turbo": "glm-5-turbo",
    "nadirclaw_glm51": "glm-5.1",
    "nadirclaw_minimax": "minimax-MiniMax-M2.7",
}


def init_agent_memory(provider: str, model_name: str = "") -> None:
    """Initialize file-based memory files for one agent.

    Args:
        provider: Agent provider ID used as the directory name.
        model_name: Optional model name written to the README file.
    """
    agent_dir = _get_agent_dir(provider)
    agent_dir.mkdir(parents=True, exist_ok=True)

    readme_path = agent_dir / "README.md"
    if not readme_path.exists():
        readme_path.write_text(
            _build_readme(provider, model_name),
            encoding="utf-8",
        )

    for filename in _MEMORY_FILENAMES:
        file_path = agent_dir / filename
        if not file_path.exists():
            file_path.write_text("", encoding="utf-8")


def load_agent_memory(provider: str, max_days: int = 3) -> str:
    """Load condensed facts and recent lessons for one agent.

    Args:
        provider: Agent provider ID.
        max_days: Maximum number of dated lesson sections to include.

    Returns:
        Combined memory text capped at 500 characters.
    """
    agent_dir = _get_agent_dir(provider)
    facts_text = _read_text(agent_dir / "facts.md").strip()
    lessons_text = _read_text(agent_dir / "lessons.md")
    recent_lessons = _extract_recent_sections(lessons_text, max_days).strip()

    parts = [part for part in (facts_text, recent_lessons) if part]
    if not parts:
        return ""

    combined = "\n\n".join(parts)
    return combined[:_MAX_LESSON_CHARS]


def write_agent_lesson(provider: str, trade_date: str, lessons: list[str]) -> None:
    """Append a dated lesson section for one agent.

    Args:
        provider: Agent provider ID.
        trade_date: Trade date in YYYY-MM-DD format.
        lessons: Lesson bullet items to append.
    """
    if not lessons:
        return

    init_agent_memory(provider, _AGENT_MODELS.get(provider, ""))
    lesson_path = _get_agent_dir(provider) / "lessons.md"
    lines = [f"## {trade_date}", *[f"- {lesson}" for lesson in lessons], ""]
    with lesson_path.open("a", encoding="utf-8") as file_obj:
        file_obj.write("\n".join(lines))


def update_agent_facts(provider: str, stats: dict[str, Any]) -> None:
    """Overwrite the historical stats section for one agent.

    Args:
        provider: Agent provider ID.
        stats: Key-value stats written as markdown bullets.
    """
    init_agent_memory(provider, _AGENT_MODELS.get(provider, ""))
    facts_path = _get_agent_dir(provider) / "facts.md"
    lines = ["## 历史统计"]
    lines.extend(f"- {key}: {value}" for key, value in stats.items())
    facts_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _extract_recent_sections(content: str, max_days: int) -> str:
    """Extract the last N dated markdown sections from lesson content."""
    if max_days <= 0 or not content.strip():
        return ""

    matches = list(_DATE_SECTION_PATTERN.finditer(content))
    if not matches:
        return ""

    sections = [
        content[matches[index].start():matches[index + 1].start()].strip()
        if index + 1 < len(matches)
        else content[matches[index].start():].strip()
        for index in range(len(matches))
    ]
    return "\n\n".join(sections[-max_days:])


def _get_agent_dir(provider: str) -> Path:
    return _MEMORY_ROOT / provider


def _build_readme(provider: str, model_name: str) -> str:
    model_line = model_name or "unknown"
    return (
        f"# {provider}\n\n"
        f"- Provider ID: {provider}\n"
        f"- Model: {model_line}\n"
    )


def _read_text(file_path: Path) -> str:
    if not file_path.exists():
        return ""
    return file_path.read_text(encoding="utf-8")
