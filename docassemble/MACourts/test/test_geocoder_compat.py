import unittest
from unittest.mock import patch

from .. import macourts


class ModernGeoCoder:
    def __init__(self):
        self.initialized_without_server = True


class LegacyGeoCoder:
    def __init__(self, **kwargs):
        if "server" not in kwargs:
            raise KeyError("server")
        self.server = kwargs["server"]


class BrokenGeoCoder:
    def __init__(self):
        raise KeyError("unrelated constructor failure")


class TestGoogleV3GeoCoderCompatibility(unittest.TestCase):
    def test_modern_geocoder_is_constructed_without_server(self):
        with patch.object(macourts, "GoogleV3GeoCoder", ModernGeoCoder):
            geocoder = macourts._make_google_v3_geocoder()

        self.assertIsInstance(geocoder, ModernGeoCoder)
        self.assertTrue(geocoder.initialized_without_server)

    def test_legacy_geocoder_gets_a_server_config_adapter(self):
        with patch.object(macourts, "GoogleV3GeoCoder", LegacyGeoCoder):
            geocoder = macourts._make_google_v3_geocoder()

        self.assertIsInstance(geocoder, LegacyGeoCoder)
        self.assertIs(geocoder.server.daconfig, macourts.daconfig)

    def test_unrelated_key_error_is_not_hidden(self):
        with patch.object(macourts, "GoogleV3GeoCoder", BrokenGeoCoder):
            with self.assertRaisesRegex(KeyError, "unrelated constructor failure"):
                macourts._make_google_v3_geocoder()


if __name__ == "__main__":
    unittest.main()
