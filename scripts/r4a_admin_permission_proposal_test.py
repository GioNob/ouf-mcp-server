import importlib.util
import json
import pathlib
import tempfile
import unittest

PATH=pathlib.Path(__file__).parents[1]/"scripts"/"r4a_admin_permission_proposal.py"
spec=importlib.util.spec_from_file_location("admin_mcp",PATH)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class AdminMcpProposalTest(unittest.TestCase):
    def test_admin_claims_require_expected_context_and_scopes(self):
        claims={
            "iss":m.ISSUER,
            "aud":["ouf-api-gateway"],
            "sub":m.ADMIN_SUB,
            "tenant_id":"ouf-lab",
            "ouf_actor_type":"HUMAN",
            "acr":"1",
            "scope":"openid mcp.connect authorization.permissions.propose"
        }
        self.assertTrue(m.validate_admin_claims(claims))
        bad=dict(claims); bad["scope"]="openid mcp.connect"
        with self.assertRaisesRegex(ValueError,"missing token scopes"):
            m.validate_admin_claims(bad)

    def test_verify_tool_requires_proposal_tool(self):
        self.assertTrue(m.verify_tool({"tools":[{"name":"authorization.permissions.propose"}]},"authorization.permissions.propose"))
        with self.assertRaisesRegex(ValueError,"required MCP tool unavailable"):
            m.verify_tool({"tools":[{"name":"ouf.system.status"}]},"authorization.permissions.propose")

    def test_load_request_is_closed_and_proposal_only(self):
        value={
            "toolName":"authorization.permissions.propose",
            "arguments":{
                "operation":"UPSERT",
                "grantId":"g",
                "grant":{"grantId":"g"}
            },
            "expectedBasePolicyRef":"ouf-lab-authorization:15"
        }
        with tempfile.TemporaryDirectory() as td:
            path=pathlib.Path(td)/"request.json"
            path.write_text(json.dumps(value))
            self.assertEqual(m.load_request(path),value)
            value["toolName"]="authorization.permissions.read"
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,"unexpected toolName"):
                m.load_request(path)

    def test_receipt_requires_pending_and_human_approval_path(self):
        result={"content":[{"type":"text","text":json.dumps({
            "proposalId":"11111111-1111-4111-8111-111111111111",
            "revision":0,
            "state":"PENDING",
            "expiresAt":"2026-09-24T10:00:00Z",
            "approvalPath":"https://api.ouf-lab.it/trusted-human/authorization/?proposal=11111111-1111-4111-8111-111111111111",
            "finalPolicyRef":None
        })}],"isError":False}
        got=m.proposal_receipt(result)
        self.assertEqual(got["state"],"PENDING")
        bad={"content":[{"type":"text","text":json.dumps({**got,"state":"PUBLISHED"})}]}
        with self.assertRaisesRegex(ValueError,"proposal is not pending"):
            m.proposal_receipt(bad)


    def test_tools_call_sets_mcp_name_header(self):
        headers=m.mcp_headers(
            "token",
            "tools/call",
            {"name":"authorization.permissions.propose","arguments":{}},
            "idem.1",
        )
        self.assertEqual(headers["Mcp-Name"],"authorization.permissions.propose")
        self.assertEqual(headers["Idempotency-Key"],"idem.1")
        listed=m.mcp_headers("token","tools/list",{})
        self.assertNotIn("Mcp-Name",listed)

    def test_idempotency_pattern(self):
        self.assertIsNotNone(m.IDEMPOTENCY_RE.fullmatch("r4a.search.giovanni:1"))
        self.assertIsNone(m.IDEMPOTENCY_RE.fullmatch("not allowed /"))
        

if __name__=="__main__":
    unittest.main()
