"""Tests for utils module."""
import pytest
from datetime import datetime, timedelta

from utils import (
    validate_wallet_address,
    InvalidWalletAddressError,
    format_currency,
    format_percentage,
    truncate_address,
    format_time_ago,
)


class TestValidateWalletAddress:
    """Test wallet address validation."""

    def test_valid_lowercase_address(self):
        """Test valid lowercase address."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        result = validate_wallet_address(addr)
        assert result == addr

    def test_valid_uppercase_address(self):
        """Test valid uppercase address is lowercased."""
        addr = "0x1234567890ABCDEF1234567890ABCDEF12345678"
        result = validate_wallet_address(addr)
        assert result == addr.lower()

    def test_valid_mixed_case_address(self):
        """Test valid mixed case address."""
        addr = "0x1234567890AbCdEf1234567890AbCdEf12345678"
        result = validate_wallet_address(addr)
        assert result == addr.lower()

    def test_empty_address(self):
        """Test empty address raises error."""
        with pytest.raises(InvalidWalletAddressError, match="cannot be empty"):
            validate_wallet_address("")

    def test_missing_0x_prefix(self):
        """Test address without 0x prefix raises error."""
        with pytest.raises(InvalidWalletAddressError, match="must start with"):
            validate_wallet_address("1234567890abcdef1234567890abcdef12345678")

    def test_too_short_address(self):
        """Test address that is too short raises error."""
        with pytest.raises(InvalidWalletAddressError, match="42 characters"):
            validate_wallet_address("0x1234")

    def test_too_long_address(self):
        """Test address that is too long raises error."""
        with pytest.raises(InvalidWalletAddressError, match="42 characters"):
            validate_wallet_address("0x1234567890abcdef1234567890abcdef12345678extra")

    def test_invalid_hex_characters(self):
        """Test address with invalid hex characters raises error."""
        with pytest.raises(InvalidWalletAddressError, match="invalid characters"):
            validate_wallet_address("0xGGGG567890abcdef1234567890abcdef12345678")


class TestFormatCurrency:
    """Test currency formatting."""

    def test_format_positive(self):
        """Test formatting positive values."""
        assert format_currency(1234.56) == "$1,234.56"

    def test_format_negative(self):
        """Test formatting negative values."""
        assert format_currency(-1234.56) == "$-1,234.56"

    def test_format_zero(self):
        """Test formatting zero."""
        assert format_currency(0) == "$0.00"

    def test_format_large_number(self):
        """Test formatting large numbers."""
        assert format_currency(1234567.89) == "$1,234,567.89"

    def test_format_small_decimals(self):
        """Test formatting rounds to 2 decimals."""
        assert format_currency(1.234) == "$1.23"


class TestFormatPercentage:
    """Test percentage formatting."""

    def test_format_decimal(self):
        """Test formatting decimal as percentage."""
        assert format_percentage(0.15) == "15.00%"

    def test_format_zero(self):
        """Test formatting zero percentage."""
        assert format_percentage(0) == "0.00%"

    def test_format_negative(self):
        """Test formatting negative percentage."""
        assert format_percentage(-0.05) == "-5.00%"


class TestTruncateAddress:
    """Test address truncation."""

    def test_truncate_long_address(self):
        """Test truncating long address."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        result = truncate_address(addr)
        assert result == "0x1234...345678"

    def test_truncate_with_custom_chars(self):
        """Test truncating with custom character count."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        result = truncate_address(addr, chars=4)
        assert result == "0x12...5678"

    def test_short_address_unchanged(self):
        """Test short address is not truncated."""
        addr = "0x1234"
        result = truncate_address(addr)
        assert result == addr


class TestFormatTimeAgo:
    """Test time ago formatting."""

    def test_seconds_ago(self):
        """Test formatting seconds ago."""
        ts = datetime.now() - timedelta(seconds=30)
        result = format_time_ago(ts)
        assert "s ago" in result

    def test_minutes_ago(self):
        """Test formatting minutes ago."""
        ts = datetime.now() - timedelta(minutes=5)
        result = format_time_ago(ts)
        assert "m ago" in result

    def test_hours_ago(self):
        """Test formatting hours ago."""
        ts = datetime.now() - timedelta(hours=3)
        result = format_time_ago(ts)
        assert "h ago" in result

    def test_days_ago(self):
        """Test formatting days ago."""
        ts = datetime.now() - timedelta(days=2)
        result = format_time_ago(ts)
        assert "d ago" in result
