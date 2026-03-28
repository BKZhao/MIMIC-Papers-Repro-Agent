"""Legacy compatibility modules.

These modules preserve deprecated execution surfaces while the main runtime
continues to converge on the paper-first agentic/profile architecture.
"""

from .pipeline import LegacyPaperReproPipeline, PaperReproPipeline

__all__ = ["LegacyPaperReproPipeline", "PaperReproPipeline"]
