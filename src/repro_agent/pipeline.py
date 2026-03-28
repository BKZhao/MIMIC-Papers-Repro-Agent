"""Compatibility facade for the legacy preset pipeline module.

The project-standard execution path is the paper-first agentic/profile stack.
This module is intentionally kept as a thin bridge so older imports continue to
resolve without making the legacy pipeline look like the primary architecture.
"""

from .legacy.pipeline import *  # noqa: F401,F403

