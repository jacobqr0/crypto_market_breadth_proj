"""
Prompty file loader and version parser.

Handles loading prompty files and extracting version metadata
for audit tracking purposes.
"""

import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Default prompts directory
PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts"


def load_prompt(agent_name: str, prompts_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load a prompty file and parse its contents.
    
    Args:
        agent_name: Name of the agent (e.g., "token_research", "orchestrator")
        prompts_dir: Optional custom prompts directory
    
    Returns:
        Dictionary with parsed prompt data including:
        - name: Agent name from file
        - version: Semantic version string
        - description: Agent description
        - model: Model configuration
        - system: System prompt content
        - user: User prompt template
    """
    prompts_path = prompts_dir or PROMPTS_DIR
    prompt_file = prompts_path / f"{agent_name}.prompty"
    
    if not prompt_file.exists():
        logger.warning(f"Prompt file not found: {prompt_file}")
        return {
            "name": agent_name,
            "version": "0.0.0",
            "description": "",
            "system": "",
            "user": "",
        }
    
    try:
        content = prompt_file.read_text()
        return _parse_prompty(content)
    except Exception as e:
        logger.error(f"Failed to load prompt {prompt_file}: {e}")
        return {
            "name": agent_name,
            "version": "0.0.0",
            "description": "",
            "system": "",
            "user": "",
        }


def _parse_prompty(content: str) -> Dict[str, Any]:
    """
    Parse a prompty file content into structured data.
    
    Prompty format:
    ---
    name: Agent Name
    version: 1.0.0
    description: Description
    model:
      api: openai
      configuration:
        model: gpt-4o
    ---
    
    system:
    System prompt content...
    
    user:
    User prompt template...
    """
    result = {
        "name": "",
        "version": "0.0.0",
        "description": "",
        "model": {},
        "system": "",
        "user": "",
    }
    
    # Split frontmatter and body
    frontmatter_match = re.match(r'^---\s*\n(.*?)\n---\s*\n(.*)$', content, re.DOTALL)
    
    if frontmatter_match:
        frontmatter = frontmatter_match.group(1)
        body = frontmatter_match.group(2)
        
        # Parse YAML frontmatter
        try:
            metadata = yaml.safe_load(frontmatter)
            if isinstance(metadata, dict):
                result["name"] = metadata.get("name", "")
                result["version"] = str(metadata.get("version", "0.0.0"))
                result["description"] = metadata.get("description", "")
                result["model"] = metadata.get("model", {})
                
                # Copy any additional metadata
                for key in metadata:
                    if key not in result:
                        result[key] = metadata[key]
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse prompty frontmatter: {e}")
        
        # Parse body sections
        _parse_body_sections(body, result)
    else:
        # No frontmatter, treat entire content as system prompt
        result["system"] = content
    
    return result


def _parse_body_sections(body: str, result: Dict[str, Any]) -> None:
    """Parse the body sections (system, user) from prompty content."""
    current_section = None
    section_content = []
    
    for line in body.split('\n'):
        # Check for section headers
        if line.strip().lower() == 'system:':
            if current_section and section_content:
                result[current_section] = '\n'.join(section_content).strip()
            current_section = 'system'
            section_content = []
        elif line.strip().lower() == 'user:':
            if current_section and section_content:
                result[current_section] = '\n'.join(section_content).strip()
            current_section = 'user'
            section_content = []
        elif current_section:
            section_content.append(line)
    
    # Save last section
    if current_section and section_content:
        result[current_section] = '\n'.join(section_content).strip()


def get_prompt_version(agent_name: str, prompts_dir: Optional[Path] = None) -> str:
    """
    Get the version string for a specific agent's prompt.
    
    Args:
        agent_name: Name of the agent
        prompts_dir: Optional custom prompts directory
    
    Returns:
        Version string (e.g., "1.0.0")
    """
    prompt_data = load_prompt(agent_name, prompts_dir)
    return prompt_data.get("version", "0.0.0")


def get_all_prompt_versions(prompts_dir: Optional[Path] = None) -> Dict[str, str]:
    """
    Get versions for all agent prompts.
    
    Args:
        prompts_dir: Optional custom prompts directory
    
    Returns:
        Dictionary mapping agent names to version strings
    """
    prompts_path = prompts_dir or PROMPTS_DIR
    
    agent_names = [
        "token_research",  # Legacy, kept for backward compatibility
        "token_screener",
        "fundamentals_analyst",
        "research_synthesizer",
        "technical_analyst",
        "macro_cycle",
        "portfolio_context",
        "orchestrator",
        "qa_risk",
        "post_mortem",
    ]
    
    versions = {}
    for agent_name in agent_names:
        versions[agent_name] = get_prompt_version(agent_name, prompts_path)
    
    return versions


def list_available_prompts(prompts_dir: Optional[Path] = None) -> list:
    """
    List all available prompty files.
    
    Args:
        prompts_dir: Optional custom prompts directory
    
    Returns:
        List of dictionaries with prompt metadata
    """
    prompts_path = prompts_dir or PROMPTS_DIR
    
    if not prompts_path.exists():
        return []
    
    prompts = []
    for prompt_file in prompts_path.glob("*.prompty"):
        prompt_data = load_prompt(prompt_file.stem, prompts_path)
        prompts.append({
            "file": prompt_file.name,
            "agent_name": prompt_file.stem,
            "name": prompt_data.get("name", ""),
            "version": prompt_data.get("version", ""),
            "description": prompt_data.get("description", ""),
        })
    
    return prompts


def get_archived_versions(agent_name: str, prompts_dir: Optional[Path] = None) -> list:
    """
    Get list of archived versions for a specific agent prompt.
    
    Args:
        agent_name: Name of the agent
        prompts_dir: Optional custom prompts directory
    
    Returns:
        List of archived version info dictionaries
    """
    prompts_path = prompts_dir or PROMPTS_DIR
    archive_path = prompts_path / "archive"
    
    if not archive_path.exists():
        return []
    
    archived = []
    pattern = f"{agent_name}_v*.prompty"
    
    for archive_file in archive_path.glob(pattern):
        # Extract version from filename (e.g., token_research_v1.0.0.prompty)
        version_match = re.search(r'_v(\d+\.\d+\.\d+)\.prompty$', archive_file.name)
        if version_match:
            version = version_match.group(1)
            archived.append({
                "file": archive_file.name,
                "version": version,
                "path": str(archive_file),
            })
    
    # Sort by version descending
    archived.sort(key=lambda x: [int(p) for p in x["version"].split(".")], reverse=True)
    
    return archived


def validate_prompt(agent_name: str, prompts_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Validate a prompty file and return any issues found.
    
    Args:
        agent_name: Name of the agent
        prompts_dir: Optional custom prompts directory
    
    Returns:
        Dictionary with validation results:
        - valid: Boolean indicating if prompt is valid
        - issues: List of issue descriptions
        - data: Parsed prompt data
    """
    prompt_data = load_prompt(agent_name, prompts_dir)
    issues = []
    
    # Check required fields
    if not prompt_data.get("version") or prompt_data["version"] == "0.0.0":
        issues.append("Missing or invalid version field")
    
    if not prompt_data.get("name"):
        issues.append("Missing name field")
    
    if not prompt_data.get("description"):
        issues.append("Missing description field")
    
    if not prompt_data.get("system"):
        issues.append("Missing system prompt content")
    
    # Validate version format
    version = prompt_data.get("version", "")
    if not re.match(r'^\d+\.\d+\.\d+$', version):
        issues.append(f"Version '{version}' is not valid semantic version (X.Y.Z)")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "data": prompt_data,
    }
