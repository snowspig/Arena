"""配置加载模块"""

from __future__ import annotations

from pathlib import Path

import yaml
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"

_cfg: dict | None = None


def load_config(path: str | Path | None = None) -> dict:
    global _cfg
    if _cfg is not None and path is None:
        return _cfg
    p = Path(path) if path else _CONFIG_PATH
    with open(p, "r", encoding="utf-8") as f:
        _cfg = yaml.safe_load(f)
    return _cfg


def reload_config() -> dict:
    """强制重新加载配置（清缓存后加载）。"""
    global _cfg
    _cfg = None
    return load_config()


def get_config() -> dict:
    if _cfg is None:
        return load_config()
    return _cfg


def get_mongo_uri() -> str:
    """从配置获取 MongoDB URI，无默认值。"""
    cfg = get_config()
    return cfg.get("mongodb", {}).get("uri") or ""


def get_mongo_db(db_key: str = "database") -> str:
    """从配置获取 MongoDB 数据库名。

    Args:
        db_key: mongodb 下的 key，默认 "database"。
    """
    cfg = get_config()
    return cfg.get("mongodb", {}).get(db_key, "")


def get_account_config(account_type: str = "simulation") -> dict:
    """获取指定账户类型的配置。"""
    cfg = get_config()
    if "accounts" in cfg and account_type in cfg["accounts"]:
        return cfg["accounts"][account_type]
    if "qmt" in cfg:
        return cfg["qmt"]
    raise ValueError(f"未找到账户配置: {account_type}")


def update_account_settings(account_type: str, settings: dict) -> dict:
    """写回账户配置到 settings.yaml，保留格式和注释。"""
    ryaml = YAML()
    ryaml.preserve_quotes = True
    ryaml.default_flow_style = False

    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = ryaml.load(f)

    if "accounts" not in data:
        data["accounts"] = CommentedMap()
    if account_type not in data["accounts"]:
        data["accounts"][account_type] = CommentedMap()

    for key in ("enabled", "account_id", "auto_reverse_repo_enabled", "reverse_repo_min_amount"):
        if key in settings:
            data["accounts"][account_type][key] = settings[key]

    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        ryaml.dump(data, f)

    reload_config()
    return get_account_config(account_type)


def update_settings(payload: dict) -> dict:
    """Write arbitrary top-level settings sections back to YAML."""
    ryaml = YAML()
    ryaml.preserve_quotes = True
    ryaml.default_flow_style = False

    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = ryaml.load(f)

    # Signal source
    if "signal_source" in payload:
        if "signal_source" not in data:
            data["signal_source"] = CommentedMap()
        for k, v in payload["signal_source"].items():
            data["signal_source"][k] = v

    # Arena
    if "arena" in payload:
        if "arena" not in data:
            data["arena"] = CommentedMap()
        for k, v in payload["arena"].items():
            if k == "providers" and isinstance(v, dict):
                if "providers" not in data["arena"]:
                    data["arena"]["providers"] = CommentedMap()
                for pname, pval in v.items():
                    if pname not in data["arena"]["providers"]:
                        data["arena"]["providers"][pname] = CommentedMap()
                    for pk, pv in pval.items():
                        data["arena"]["providers"][pname][pk] = pv
            else:
                data["arena"][k] = v

    # MongoDB
    if "mongodb" in payload:
        if "mongodb" not in data:
            data["mongodb"] = CommentedMap()
        for k, v in payload["mongodb"].items():
            data["mongodb"][k] = v

    # Accounts
    if "accounts" in payload:
        if "accounts" not in data:
            data["accounts"] = CommentedMap()
        for atype, avals in payload["accounts"].items():
            if atype not in data["accounts"]:
                data["accounts"][atype] = CommentedMap()
            for ak, av in avals.items():
                data["accounts"][atype][ak] = av

    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        ryaml.dump(data, f)

    reload_config()
    return get_config()
