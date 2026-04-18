"""
test_ai_advisor.py
===================
Tests for the ai_advisor/advisor.py
"""

from unittest.mock import MagicMock, patch
import pytest
from ai_advisor.advisor import generate_portfolio_review

class TestGeneratePortfolioReview:
    
    @patch.dict("os.environ", clear=True)
    def test_missing_api_key_returns_error(self):
        """When GEMINI_API_KEY is missing, should return an error."""
        result = generate_portfolio_review("Pankaj")
        assert "Error: GEMINI_API_KEY not found" in result

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "GEMINI_MODEL": "gemini-3-flash-preview"})
    @patch("ai_advisor.advisor.LLM", side_effect=Exception("API limit"))
    def test_llm_init_failure_returns_error(self, mock_llm):
        """When LLM initialization fails, should return an error."""
        result = generate_portfolio_review("Pankaj")
        assert "Error initializing Google Gemini LLM" in result

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "GEMINI_MODEL": "gemini-3-flash-preview"})
    @patch("ai_advisor.advisor.LLM")
    @patch("ai_advisor.advisor.Agent")
    @patch("ai_advisor.advisor.Task")
    @patch("ai_advisor.advisor.Crew")
    def test_crew_exception_returns_error(self, mock_crew, mock_task, mock_agent, mock_llm):
        """When Crew.kickoff() fails, should log and return error."""
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff.side_effect = Exception("Agent error")
        mock_crew.return_value = mock_crew_instance

        result = generate_portfolio_review("Pankaj")
        assert "Agent framework execution failed: Agent error" in result

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "GEMINI_MODEL": "gemini-3-flash-preview"})
    @patch("ai_advisor.advisor.LLM")
    @patch("ai_advisor.advisor.Agent")
    @patch("ai_advisor.advisor.Task")
    @patch("ai_advisor.advisor.Crew")
    def test_successful_run_returns_report(self, mock_crew, mock_task, mock_agent, mock_llm):
        """When Crew.kickoff() succeeds, should return the markdown report."""
        mock_crew_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.raw = "# Portfolio Review\nAll good."
        mock_crew_instance.kickoff.return_value = mock_result
        mock_crew.return_value = mock_crew_instance

        result = generate_portfolio_review("Pankaj")
        assert "# Portfolio Review" in result
        assert "All good." in result
