"""Compatibility helpers for docassemble.MACourts."""

__version__ = "1.4.0"


def _patch_docassemble_geocoder_compatibility() -> None:
    """
    Keep docassemble.MACourts importable across docassemble versions.

    Older docassemble.MACourts code imports the private
    ``docassemble.base.functions.server`` symbol and passes it as ``server=``
    to ``GoogleV3GeoCoder``. Newer docassemble versions no longer expose that
    symbol and no longer need the keyword. Patch those APIs before
    ``docassemble.MACourts.macourts`` imports them.
    """
    try:
        import inspect
        import docassemble.base.functions as functions
        import docassemble.base.geocode as geocode
    except Exception:
        return

    if not hasattr(functions, "server"):
        functions.server = None

    google_v3_geocoder = getattr(geocode, "GoogleV3GeoCoder", None)
    if google_v3_geocoder is None:
        return

    try:
        parameters = inspect.signature(google_v3_geocoder).parameters
    except (TypeError, ValueError):
        parameters = {}

    if "server" in parameters:
        return

    class GoogleV3GeoCoder(google_v3_geocoder):  # type: ignore[misc, valid-type]
        def __init__(self, *args, server=None, **kwargs):
            super().__init__(*args, **kwargs)

    GoogleV3GeoCoder.__name__ = google_v3_geocoder.__name__
    GoogleV3GeoCoder.__qualname__ = google_v3_geocoder.__qualname__
    GoogleV3GeoCoder.__module__ = google_v3_geocoder.__module__
    geocode.GoogleV3GeoCoder = GoogleV3GeoCoder


_patch_docassemble_geocoder_compatibility()
