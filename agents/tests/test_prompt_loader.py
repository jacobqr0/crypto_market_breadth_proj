"""
Tests for the prompt loader utility.
"""

import pytest
from pathlib import Path
import tempfile
import shutil

from agents.utils.prompt_loader import (
    load_prompt,
    get_prompt_version,
    get_all_prompt_versions,
    list_available_prompts,
    validate_prompt,
    _parse_prompty,
)


# Sample prompty content for testing
SAMPLE_PROMPTY = """---
name: Test Agent
version: 1.2.3
description: A test agent for unit testing
model:
  api: openai
  configuration:
    model: gpt-4o
tags:
  - test
---

system:
You are a test agent. Your role is to help with testing.

user:
{{task_description}}
"""


MINIMAL_PROMPTY = """---
name: Minimal Agent
version: 0.1.0
---

system:
Minimal system prompt.
"""


INVALID_VERSION_PROMPTY = """---
name: Bad Version Agent
version: invalid
---

system:
System prompt.
"""


class TestParsePrompty:
    """Tests for _parse_prompty function."""
    
    def test_parse_full_prompty(self):
        """Test parsing a complete prompty file."""
        result = _parse_prompty(SAMPLE_PROMPTY)
        
        assert result["name"] == "Test Agent"
        assert result["version"] == "1.2.3"
        assert result["description"] == "A test agent for unit testing"
        assert result["model"]["api"] == "openai"
        assert "test agent" in result["system"].lower()
        assert "{{task_description}}" in result["user"]
    
    def test_parse_minimal_prompty(self):
        """Test parsing a minimal prompty file."""
        result = _parse_prompty(MINIMAL_PROMPTY)
        
        assert result["name"] == "Minimal Agent"
        assert result["version"] == "0.1.0"
        assert "minimal system prompt" in result["system"].lower()
    
    def test_parse_no_frontmatter(self):
        """Test parsing content without frontmatter."""
        content = "Just some plain text content"
        result = _parse_prompty(content)
        
        assert result["system"] == content
        assert result["version"] == "0.0.0"


class TestLoadPrompt:
    """Tests for load_prompt function."""
    
    @pytest.fixture
    def temp_prompts_dir(self):
        """Create a temporary prompts directory."""
        temp_dir = Path(tempfile.mkdtemp())
        
        # Create a test prompt file
        test_prompt = temp_dir / "test_agent.prompty"
        test_prompt.write_text(SAMPLE_PROMPTY)
        
        yield temp_dir
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    def test_load_existing_prompt(self, temp_prompts_dir):
        """Test loading an existing prompt file."""
        result = load_prompt("test_agent", prompts_dir=temp_prompts_dir)
        
        assert result["name"] == "Test Agent"
        assert result["version"] == "1.2.3"
    
    def test_load_nonexistent_prompt(self, temp_prompts_dir):
        """Test loading a non-existent prompt file."""
        result = load_prompt("nonexistent", prompts_dir=temp_prompts_dir)
        
        assert result["name"] == "nonexistent"
        assert result["version"] == "0.0.0"


class TestGetPromptVersion:
    """Tests for get_prompt_version function."""
    
    @pytest.fixture
    def temp_prompts_dir(self):
        """Create a temporary prompts directory."""
        temp_dir = Path(tempfile.mkdtemp())
        
        test_prompt = temp_dir / "versioned_agent.prompty"
        test_prompt.write_text(SAMPLE_PROMPTY)
        
        yield temp_dir
        
        shutil.rmtree(temp_dir)
    
    def test_get_version(self, temp_prompts_dir):
        """Test getting version from prompt file."""
        version = get_prompt_version("versioned_agent", prompts_dir=temp_prompts_dir)
        assert version == "1.2.3"
    
    def test_get_version_nonexistent(self, temp_prompts_dir):
        """Test getting version for non-existent file."""
        version = get_prompt_version("nonexistent", prompts_dir=temp_prompts_dir)
        assert version == "0.0.0"


class TestGetAllPromptVersions:
    """Tests for get_all_prompt_versions function."""
    
    def test_get_all_versions(self):
        """Test getting versions for all standard agents."""
        # This test uses the actual prompts directory
        versions = get_all_prompt_versions()
        
        expected_agents = [
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
        
        for agent in expected_agents:
            assert agent in versions


class TestValidatePrompt:
    """Tests for validate_prompt function."""
    
    @pytest.fixture
    def temp_prompts_dir(self):
        """Create a temporary prompts directory."""
        temp_dir = Path(tempfile.mkdtemp())
        
        # Valid prompt
        valid_prompt = temp_dir / "valid_agent.prompty"
        valid_prompt.write_text(SAMPLE_PROMPTY)
        
        # Invalid version prompt
        invalid_prompt = temp_dir / "invalid_agent.prompty"
        invalid_prompt.write_text(INVALID_VERSION_PROMPTY)
        
        yield temp_dir
        
        shutil.rmtree(temp_dir)
    
    def test_validate_valid_prompt(self, temp_prompts_dir):
        """Test validation of a valid prompt."""
        result = validate_prompt("valid_agent", prompts_dir=temp_prompts_dir)
        
        assert result["valid"] is True
        assert len(result["issues"]) == 0
    
    def test_validate_invalid_prompt(self, temp_prompts_dir):
        """Test validation of a prompt with invalid version."""
        result = validate_prompt("invalid_agent", prompts_dir=temp_prompts_dir)
        
        assert result["valid"] is False
        assert any("version" in issue.lower() for issue in result["issues"])


class TestListAvailablePrompts:
    """Tests for list_available_prompts function."""
    
    def test_list_prompts(self):
        """Test listing available prompts."""
        # Uses actual prompts directory
        prompts = list_available_prompts()
        
        assert len(prompts) >= 10  # We have 10+ prompts (including new token research chain)
        
        # Check structure
        for prompt in prompts:
            assert "file" in prompt
            assert "agent_name" in prompt
            assert "version" in prompt
