"""AI Signal Arena —— AI 信号提供者。"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import httpx
from loguru import logger

_DEFAULT_TIMEOUT = 300.0
_MAX_RETRIES = 2


class AISignalProvider(ABC):
    """AI 信号提供者基类。"""

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """提供者唯一标识，如 'vllm_trader_pro'。"""

    @abstractmethod
    def generate_picks(
        self,
        system_prompt: str,
        user_prompt: str,
        trade_date: date,
    ) -> dict[str, Any]:
        """调用 AI 模型，返回 picks 结果。"""


class OpenAICompatProvider(AISignalProvider):
    """基于 OpenAI-compatible chat/completions API 的提供者。"""

    def __init__(
        self,
        provider_id: str,
        base_url: str,
        model: str,
        api_key: str = "none",
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        self._provider_id = provider_id
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._timeout = timeout

    @property
    def provider_id(self) -> str:
        return self._provider_id

    def generate_picks(
        self,
        system_prompt: str,
        user_prompt: str,
        trade_date: date,
    ) -> dict[str, Any]:
        url = f"{self._base_url}/chat/completions"
        headers = self._build_headers()
        payload = self._build_payload(system_prompt, user_prompt)

        logger.info(
            f"[{self._provider_id}] 调用 AI: model={self._model}, "
            f"trade_date={trade_date.isoformat()}"
        )

        try:
            response_text, actual_model, attempts = self._call_with_retry(
                url, headers, payload,
            )
            fallback = self._check_fallback(actual_model)
            return self._parse_response(
                response_text, trade_date, actual_model, attempts, fallback,
            )
        except RuntimeError as e:
            logger.error(f"[{self._provider_id}] {e}")
            return _empty_picks(
                provider_id=self._provider_id,
                trade_date=trade_date,
                model=self._model,
                base_url=self._base_url,
                attempts=_MAX_RETRIES + 1,
                error_msg=str(e),
            )

    def _build_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }

    def _build_payload(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        return {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 200000,
            "nadirclaw_routing": {"strategy": "direct", "allow_fallback": False},
        }

    def _call_with_retry(
        self,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> tuple[str, str | None, int]:
        """调用 API 并返回 (content, actual_model, attempts)。

        模型 fallback（actual_model != requested_model）也会触发重试。

        Returns:
            元组: (响应文本, API 响应中的 model 字段, 实际尝试次数)
        """
        last_error: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 2):
            try:
                resp = httpx.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                actual_model = data.get("model")

                # 检查模型 fallback
                if self._check_fallback(actual_model):
                    fallback_err = (
                        f"模型 fallback: 请求={self._model}, "
                        f"实际={actual_model}"
                    )
                    logger.warning(
                        f"[{self._provider_id}] 第 {attempt} 次调用 "
                        f"{fallback_err}，触发重试"
                    )
                    raise RuntimeError(fallback_err)

                logger.info(
                    f"[{self._provider_id}] AI 返回成功 "
                    f"(attempt={attempt}, chars={len(content)}, "
                    f"finish_reason={data['choices'][0].get('finish_reason')}, "
                    f"actual_model={actual_model})"
                )
                return content, actual_model, attempt
            except Exception as e:
                last_error = e
                logger.warning(
                    f"[{self._provider_id}] 第 {attempt} 次调用失败: {e}"
                )
        raise RuntimeError(
            f"[{self._provider_id}] AI 调用最终失败: {last_error}"
        )

    def _parse_response(
        self,
        response_text: str,
        trade_date: date,
        actual_model: str | None,
        attempts: int,
        fallback_detected: bool,
    ) -> dict[str, Any]:
        picks, parse_mode, error_message = _parse_picks(response_text)
        if error_message:
            logger.error(f"[{self._provider_id}] {error_message}")
            logger.debug(f"原始响应前 500 字符: {response_text[:500]}")
            return _empty_picks(
                provider_id=self._provider_id,
                trade_date=trade_date,
                model=self._model,
                base_url=self._base_url,
                attempts=attempts,
                error_msg=error_message,
                actual_model=actual_model,
                fallback_detected=fallback_detected,
            )

        logger.info(
            f"[{self._provider_id}] 解析到 {len(picks)} 只 picks "
            f"(mode={parse_mode})"
        )
        return {
            "provider": self._provider_id,
            "trade_date": trade_date.isoformat(),
            "requested_model": self._model,
            "actual_model": actual_model,
            "base_url": self._base_url,
            "attempts": attempts,
            "status": "success",
            "fallback_detected": fallback_detected,
            "parse_mode": parse_mode,
            "response_truncated": _detect_truncation(response_text),
            "picks": picks,
            "raw_response": response_text,
        }

    def _check_fallback(self, actual_model: str | None) -> bool:
        """检查响应模型是否与请求模型不一致。"""
        return actual_model is not None and actual_model != self._model


class AnthropicCompatProvider(AISignalProvider):
    """基于 Anthropic messages API 的提供者（minimax 等兼容端点）。"""

    def __init__(
        self,
        provider_id: str,
        base_url: str,
        model: str,
        api_key: str = "none",
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        self._provider_id = provider_id
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._timeout = timeout

    @property
    def provider_id(self) -> str:
        return self._provider_id

    def generate_picks(
        self,
        system_prompt: str,
        user_prompt: str,
        trade_date: date,
    ) -> dict[str, Any]:
        url = f"{self._base_url}/v1/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self._api_key,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": self._model,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
            "max_tokens": 8192,
        }

        logger.info(
            f"[{self._provider_id}] 调用 AI (anthropic): "
            f"model={self._model}, trade_date={trade_date.isoformat()}"
        )

        try:
            response_text, actual_model, attempts = self._call_anthropic_with_retry(
                url, headers, payload,
            )
            return self._build_success(
                response_text, trade_date, actual_model, attempts,
            )
        except RuntimeError as e:
            logger.error(f"[{self._provider_id}] {e}")
            return _empty_picks(
                provider_id=self._provider_id,
                trade_date=trade_date,
                model=self._model,
                base_url=self._base_url,
                attempts=_MAX_RETRIES + 1,
                error_msg=str(e),
            )

    def _call_anthropic_with_retry(
        self,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> tuple[str, str | None, int]:
        last_error: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 2):
            try:
                resp = httpx.post(
                    url, headers=headers, json=payload,
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                actual_model = data.get("model")
                text = self._extract_text(data)
                if not text:
                    raise RuntimeError("Anthropic 响应 content 为空")
                logger.info(
                    f"[{self._provider_id}] AI 返回成功 "
                    f"(attempt={attempt}, chars={len(text)}, "
                    f"actual_model={actual_model})"
                )
                return text, actual_model, attempt
            except Exception as e:
                last_error = e
                logger.warning(
                    f"[{self._provider_id}] 第 {attempt} 次调用失败: {e}"
                )
        raise RuntimeError(f"[{self._provider_id}] AI 调用最终失败: {last_error}")

    @staticmethod
    def _extract_text(data: dict[str, Any]) -> str:
        """从 Anthropic content 数组中提取文本。"""
        content_blocks = data.get("content", [])
        parts = []
        for block in content_blocks:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "".join(parts)

    def _build_success(
        self,
        response_text: str,
        trade_date: date,
        actual_model: str | None,
        attempts: int,
    ) -> dict[str, Any]:
        picks, parse_mode, error_message = _parse_picks(response_text)
        if error_message:
            logger.error(f"[{self._provider_id}] {error_message}")
            return _empty_picks(
                provider_id=self._provider_id,
                trade_date=trade_date,
                model=self._model,
                base_url=self._base_url,
                attempts=attempts,
                error_msg=error_message,
                actual_model=actual_model,
            )
        logger.info(
            f"[{self._provider_id}] 解析到 {len(picks)} 只 picks "
            f"(mode={parse_mode})"
        )
        return {
            "provider": self._provider_id,
            "trade_date": trade_date.isoformat(),
            "requested_model": self._model,
            "actual_model": actual_model,
            "base_url": self._base_url,
            "attempts": attempts,
            "status": "success",
            "fallback_detected": False,
            "parse_mode": parse_mode,
            "response_truncated": _detect_truncation(response_text),
            "picks": picks,
            "raw_response": response_text,
        }


def create_provider_from_config(provider_name: str) -> AISignalProvider:
    """从 config/settings.yaml 的 arena.providers 创建 provider 实例。"""
    from .config import get_config

    cfg = get_config()
    providers_cfg = cfg.get("arena", {}).get("providers", {})
    if provider_name not in providers_cfg:
        raise ValueError(f"未找到 provider 配置: {provider_name}")

    pcfg = providers_cfg[provider_name]
    if not pcfg.get("enabled", True):
        raise ValueError(f"provider {provider_name} 已禁用")

    provider_type = pcfg.get("type", "nadirclaw")
    provider_cls = AnthropicCompatProvider if provider_type == "anthropic" else OpenAICompatProvider
    return provider_cls(
        provider_id=provider_name,
        base_url=pcfg["base_url"],
        model=pcfg["model"],
        api_key=pcfg.get("api_key", "none"),
        timeout=float(pcfg.get("timeout", _DEFAULT_TIMEOUT)),
    )


def _extract_json_block(text: str) -> str:
    """从 AI 响应中提取 JSON 块。

    支持四种情况:
    1. 纯 JSON（直接返回）
    2. markdown code fence 包裹
    3. Qwopus 思维链格式（堆堆思考过程 + 分隔符 + 正文）
    4. 通用思维链文本中嵌套的 JSON
    """
    stripped = text.strip()

    # 剥离 Qwopus 思维链格式
    # 格式: "堆堆思考过程：...\n\n\n<正文>"
    thinking_markers = ["堆堆思考过程：", "思考过程：", "<think"]
    for marker in thinking_markers:
        idx = stripped.find(marker)
        if idx != -1:
            remaining = stripped[idx:]
            sep_patterns = ["\n\n\n", "\n\n```", "\n```", "\n\n{"]
            for sep in sep_patterns:
                sep_idx = remaining.find(sep)
                if sep_idx != -1:
                    if sep.startswith("\n```"):
                        stripped = remaining[sep_idx + 1:].strip()
                    elif sep.endswith("{"):
                        stripped = remaining[sep_idx + 2:].strip()
                    else:
                        stripped = remaining[sep_idx:].strip()
                    break
            else:
                parts = remaining.split("\n\n")
                if len(parts) > 1:
                    stripped = parts[-1].strip()
            break

    # markdown code fence
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        start = 1
        end = len(lines)
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].strip().startswith("```"):
                end = i
                break
        stripped = "\n".join(lines[start:end])
        stripped = stripped.strip()

    # 已经是合法 JSON
    if stripped.startswith("{") or stripped.startswith("["):
        return stripped

    # 从文本中找 JSON 对象或数组（优先找 picks 相关的）
    json_patterns = [
        r'\{[^{}]*"picks"\s*:\s*\[.*\][^{}]*\}',
        r'\{[^{}]*"picks"\s*:\s*\[.*',
        r'(\{(?:[^{}]|"(?:[^"\\]|\\.)*")*\})',
    ]
    for pattern in json_patterns:
        matches = re.findall(pattern, stripped, re.DOTALL)
        if matches:
            return matches[-1].strip()

def _parse_picks(response_text: str) -> tuple[list[dict[str, Any]], str, str | None]:
    markdown_picks = _extract_markdown_picks(response_text)
    if markdown_picks:
        return markdown_picks, "markdown", None

    content = _extract_json_block(response_text)
    try:
        parsed = json.loads(content)
        picks = parsed if isinstance(parsed, list) else parsed.get("picks", [])
        if not isinstance(picks, list):
            return [], "json", "picks 不是列表"
        return picks, "json", None
    except json.JSONDecodeError as error:
        return [], "none", f"JSON 和 markdown 均解析失败: {error}"


def _extract_markdown_picks(text: str) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []
    pattern = re.compile(
        r"stock_code\s*[:：]\s*([0-9]{6}\.(?:SH|SZ))\s*\|\s*"
        r"confidence\s*[:：]\s*(0(?:\.\d+)?|1(?:\.0+)?|0?\.\d+)\s*\|\s*"
        r"reason\s*[:：]\s*(.+)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        stock_code, confidence, reason = match.groups()
        reason = reason.strip().rstrip("`,")
        picks.append(
            {
                "stock_code": stock_code.upper(),
                "confidence": float(confidence),
                "reason": reason,
            }
        )
    return picks


def _detect_truncation(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if stripped.startswith("{") and not stripped.endswith("}"):
        return True
    if stripped.startswith("[") and not stripped.endswith("]"):
        return True
    if stripped.startswith("```") and stripped.count("```") % 2 != 0:
        return True
    if "stock_code:" in stripped.lower():
        last_line = stripped.rstrip().split("\n")[-1].strip()
        if last_line.startswith("-") and "reason:" not in last_line.lower():
            return True
    return False


def _empty_picks(
    provider_id: str,
    trade_date: date,
    model: str,
    base_url: str,
    attempts: int,
    error_msg: str,
    actual_model: str | None = None,
    fallback_detected: bool = False,
) -> dict[str, Any]:
    return {
        "provider": provider_id,
        "trade_date": trade_date.isoformat(),
        "requested_model": model,
        "actual_model": actual_model,
        "base_url": base_url,
        "attempts": attempts,
        "status": "failed",
        "fallback_detected": fallback_detected,
        "error_message": error_msg,
        "picks": [],
        "raw_response": "",
    }
