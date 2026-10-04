"""Installable deterministic business example for the Toolframe Jobs plugin."""

from .plugin import SERVICE_NAME, plugin
from .service import DurableCounter

__all__ = ["SERVICE_NAME", "DurableCounter", "plugin"]
