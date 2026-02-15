"""
CrewAI Multi-Agent Investment System.

This package contains:
- crew.py: Core 6-agent crew for investment decisions
- post_mortem.py: Separate Post-Mortem Architect for meta-learning
- tools/: Agent tools for data access
- utils/: Report generation and prompt loading utilities
"""

__all__ = []

# Lazy imports to avoid import errors if dependencies aren't installed
def _lazy_import():
    """Lazy import crew and post_mortem modules."""
    global investment_crew, run_investment_crew, run_meta_learning
    
    try:
        from agents.crew import investment_crew, run_investment_crew
        from agents.post_mortem import run_meta_learning
        
        __all__.extend(["investment_crew", "run_investment_crew", "run_meta_learning"])
    except ImportError as e:
        import logging
        logging.warning(f"Could not import agent modules: {e}")
        logging.warning("Install required dependencies with: pip install -r requirements.txt")


# Attempt lazy import
try:
    _lazy_import()
except Exception:
    pass  # Silently fail if imports aren't possible
