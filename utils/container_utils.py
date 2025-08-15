"""Utility helpers for container number handling."""


def normalize_container_number(number: str) -> str:
    """Normalize container numbers for comparisons.

    Removes all whitespace characters and uppercases the result. If ``number`` is
    falsy or not a string, an empty string is returned.

    Args:
        number: The container number to normalize.

    Returns:
        Normalized container number.
    """

    if not isinstance(number, str):
        return ""
    # ``split`` without arguments removes all whitespace characters.
    return "".join(number.split()).upper()

