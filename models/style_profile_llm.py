"""LLM JSON mode fragrance style profiler.

Classifies a product/fragrance description into a strict `{style, mood, color}`
shape using OpenAI Chat Completions JSON mode and Pydantic validation.
"""

from __future__ import annotations

import json
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

StyleCategory = Literal[
    "fresh_clean",
    "floral_romantic",
    "woody_earthy",
    "amber_warm",
    "spicy_oriental",
    "gourmand_sweet",
    "aquatic_marine",
]

STYLE_CATEGORIES: dict[str, str] = {
    "fresh_clean": "산뜻하고 깨끗한 시트러스, 화이트 머스크, 비누 느낌",
    "floral_romantic": "장미, 자스민, 아이리스 등 부드럽고 로맨틱한 플로럴 느낌",
    "woody_earthy": "시더, 샌달우드, 베티버, 패출리 등 차분한 우디/흙내음",
    "amber_warm": "앰버, 바닐라, 벤조인, 랩다넘 등 따뜻하고 관능적인 느낌",
    "spicy_oriental": "사프란, 시나몬, 핑크페퍼, 우드 등 이국적이고 강렬한 향신료 느낌",
    "gourmand_sweet": "캔디, 과일, 디저트, 크리미함 등 달콤하고 먹음직한 느낌",
    "aquatic_marine": "물, 바다, 투명함, 차가운 공기처럼 시원한 느낌",
}

FEW_SHOT_EXAMPLES: list[dict[str, str]] = [
    {
        "input": "Sparkling bergamot, clean white musk, and a transparent citrus trail.",
        "output": '{"style":"fresh_clean","mood":"crisp and polished","color":"clear white"}',
    },
    {
        "input": "A bouquet of rose and jasmine with soft powdery petals and a romantic aura.",
        "output": '{"style":"floral_romantic","mood":"soft romantic","color":"blush pink"}',
    },
    {
        "input": "Dry cedarwood, earthy patchouli, vetiver, and a grounded forest-like depth.",
        "output": '{"style":"woody_earthy","mood":"calm grounded","color":"deep brown"}',
    },
    {
        "input": "Warm amber, vanilla, benzoin, and labdanum create a sensual evening glow.",
        "output": '{"style":"amber_warm","mood":"sensual warm","color":"golden amber"}',
    },
    {
        "input": "Saffron, cinnamon, oud wood, and pepper form an exotic intense signature.",
        "output": '{"style":"spicy_oriental","mood":"bold mysterious","color":"dark burgundy"}',
    },
    {
        "input": "Tutti-frutti candy, creamy musk, pear, and a playful edible sweetness.",
        "output": '{"style":"gourmand_sweet","mood":"playful sweet","color":"candy peach"}',
    },
    {
        "input": "Marine breeze, watery freshness, salt air, and a transparent blue trail.",
        "output": '{"style":"aquatic_marine","mood":"cool airy","color":"aqua blue"}',
    },
]


class StyleProfile(BaseModel):
    """Strict output schema for LLM classification."""

    model_config = ConfigDict(extra="forbid")

    style: StyleCategory = Field(description="One of the predefined style categories.")
    mood: str = Field(min_length=1, max_length=60, description="Short mood phrase.")
    color: str = Field(min_length=1, max_length=40, description="Representative color phrase.")

    @field_validator("mood", "color")
    @classmethod
    def normalize_short_text(cls, value: str) -> str:
        value = " ".join(value.strip().split())
        if not value:
            raise ValueError("value must not be empty")
        return value


class ChatCompletionsClient(Protocol):
    """Protocol for OpenAI-compatible `client.chat.completions.create`."""

    chat: Any


def build_style_prompt(description: str) -> list[dict[str, str]]:
    """Build few-shot prompt messages for JSON mode."""
    categories = "\n".join(f"- {key}: {desc}" for key, desc in STYLE_CATEGORIES.items())
    shots = "\n".join(
        f"Input: {example['input']}\nOutput: {example['output']}"
        for example in FEW_SHOT_EXAMPLES
    )
    return [
        {
            "role": "system",
            "content": (
                "You classify fragrance/product descriptions. "
                "Return only a valid JSON object with exactly these keys: style, mood, color. "
                "Do not include markdown, comments, arrays, or extra keys.\n\n"
                f"Allowed style values:\n{categories}"
            ),
        },
        {
            "role": "user",
            "content": (
                "Few-shot examples:\n"
                f"{shots}\n\n"
                "Now classify this description. Return JSON only.\n"
                f"Description: {description.strip()}"
            ),
        },
    ]


def _extract_message_content(response: Any) -> str:
    """Extract assistant content from OpenAI SDK object or dict response."""
    if isinstance(response, dict):
        return response["choices"][0]["message"]["content"]
    return response.choices[0].message.content


def _parse_and_validate(raw_content: str) -> StyleProfile:
    """Parse raw JSON content and validate it with Pydantic."""
    parsed = json.loads(raw_content)
    return StyleProfile.model_validate(parsed)


def classify_style_profile(
    description: str,
    client: ChatCompletionsClient,
    model: str = "gpt-4o-mini",
    max_retries: int = 3,
) -> StyleProfile:
    """Classify text into `{style, mood, color}` with up to 3 validation retries.

    Retries are triggered only by JSON parsing or Pydantic validation failures.
    Transport/API exceptions are allowed to bubble up because they are not parse
    failures and usually need caller-level handling.
    """
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")

    description = description.strip()
    if not description:
        raise ValueError("description must not be empty")

    messages = build_style_prompt(description)
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        raw_content = _extract_message_content(response)
        try:
            return _parse_and_validate(raw_content)
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            last_error = exc
            messages.append({"role": "assistant", "content": raw_content})
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Previous output failed JSON parsing or schema validation. "
                        "Retry with only a JSON object containing exactly style, mood, color. "
                        f"Attempt {attempt + 1} of {max_retries}."
                    ),
                }
            )

    raise ValueError(f"Failed to parse and validate style profile after {max_retries} attempts") from last_error


__all__ = [
    "FEW_SHOT_EXAMPLES",
    "STYLE_CATEGORIES",
    "StyleCategory",
    "StyleProfile",
    "build_style_prompt",
    "classify_style_profile",
]
