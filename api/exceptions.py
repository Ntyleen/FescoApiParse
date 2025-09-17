"""Common API client exceptions."""


class FescoApiError(Exception):
    """Base exception for FESCO API errors."""


class AuthenticationError(FescoApiError):
    """Raised when authentication fails."""


class ApiRequestError(FescoApiError):
    """Raised when an API request fails."""
