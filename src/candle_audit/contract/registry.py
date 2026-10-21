"""Shared error type only; this project does not register research data."""


class RegistryError(RuntimeError):
    """An exact data declaration failed its contract."""
