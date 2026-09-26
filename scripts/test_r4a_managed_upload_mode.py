import unittest

from scripts.r4a_managed_upload_mode import expected_environment


class ManagedUploadEnvironmentTests(unittest.TestCase):
    def test_opt_in_and_probe_do_not_change_original_environment(self):
        original = ["MCP_OIDC_CLIENT_ID=ouf-mcp-server", "MCP_DATABASE_URL=private"]
        self.assertEqual(expected_environment(original, "off", None), original)
        self.assertEqual(expected_environment(original, "probe", None),
                         [*original, "MCP_MANAGED_UPLOAD_ENABLED=probe"])
        self.assertEqual(expected_environment(original, "enabled", None),
                         [*original, "MCP_MANAGED_UPLOAD_ENABLED=true"])
        self.assertEqual(len(original), 2)
        probe = expected_environment(original, "probe", None)
        self.assertEqual(expected_environment(probe, "enabled", None),
                         [*original, "MCP_MANAGED_UPLOAD_ENABLED=true"])
        with self.assertRaises(ValueError):
            expected_environment(probe, "off", None)

    def test_enabled_mode_rejects_static_origins_and_existing_origin_env(self):
        original = ["MCP_OIDC_CLIENT_ID=ouf-mcp-server"]
        for mode, origin in (("probe", "https://files.example.org"),
                             ("off", "https://files.example.org"),
                             ("enabled", "http://files.example.org"),
                             ("enabled", "https://localhost"),
                             ("enabled", "https://127.0.0.1"),
                             ("enabled", "https://files.example.org/private"),
                             ("enabled", "https://files.example.org?token=secret"),
                             ("enabled", "https://user:secret@files.example.org"),
                             ("enabled", "https://*.example.org")):
            with self.subTest(mode=mode, origin=origin), self.assertRaises(ValueError):
                expected_environment(original, mode, origin)
        with self.assertRaises(ValueError):
            expected_environment([*original, "MCP_HOST_FILE_ORIGINS=https://old.example.org"],
                                 "enabled", None)


if __name__ == "__main__":
    unittest.main()
