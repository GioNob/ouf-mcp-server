import unittest

from scripts.r4a_managed_upload_mode import expected_environment


class ManagedUploadEnvironmentTests(unittest.TestCase):
    def test_first_party_picker_requires_exact_https_path_and_one_mode(self):
        original = ["MCP_OIDC_CLIENT_ID=ouf-mcp-server", "MCP_MANAGED_UPLOAD_ENABLED=probe"]
        link = "https://api.ouf-lab.it/trusted-human/managed-files/"
        expected = ["MCP_OIDC_CLIENT_ID=ouf-mcp-server", "MCP_MANAGED_UPLOAD_ENABLED=picker",
                    "MCP_MANAGED_FILE_PICKER_URL=" + link]
        self.assertEqual(expected_environment(original, "picker", None, link), expected)
        self.assertEqual(expected_environment(expected, "picker", None, link), expected)
        for invalid in ("http://api.ouf-lab.it/trusted-human/managed-files/",
                        "https://api.ouf-lab.it/trusted-human/managed-files/?redirect=bad",
                        "https://api.ouf-lab.it/other"):
            with self.assertRaises(ValueError):
                expected_environment(original, "picker", None, invalid)
        with self.assertRaises(ValueError):
            expected_environment(expected, "off", None)

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
        enabled = expected_environment(probe, "enabled", None)
        self.assertEqual(expected_environment(enabled, "enabled", None), enabled)
        self.assertEqual(expected_environment(enabled, "probe", None), probe)
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
