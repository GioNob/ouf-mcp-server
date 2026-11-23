"""Exercise the unmodified MCP server over its production HTTP routes."""

from http.client import HTTPConnection
import json
from pathlib import Path
import sys
import time
from uuid import UUID, uuid4


PROTOCOL = "2026-07-28"
IDENTITY = {
    "X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "d080-fixture",
    "X-OUF-Service-Principal": "mcp", "X-OUF-Principal-ID": "analyst",
    "X-OUF-Tenant-ID": "ci", "X-OUF-Actor-Type": "HUMAN",
    "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "fixture",
    "X-OUF-Token-Audience": "gateway",
    "X-OUF-Granted-Scopes": "ouf.managed-source.preview ouf.semantic.discovery ouf.system.status",
}


def request(port, path, data=None, headers=None, expected=200):
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        body = json.dumps(data) if data is not None else None
        conn.request("POST" if body is not None else "GET", path, body=body,
                     headers={"Content-Type": "application/json", **(headers or {})})
        response = conn.getresponse()
        if response.status != expected:
            raise RuntimeError(f"{path}: expected {expected}, received {response.status}")
        return json.load(response) if expected == 200 else None
    finally:
        conn.close()


def wait_ready(owner):
    for attempt in range(30):
        try:
            if owner:
                request(8081, "/dispatches")
            else:
                request(8080, "/health/ready", expected=204)
            return
        except ConnectionRefusedError:
            if attempt == 29:
                raise
            time.sleep(1)


def rpc(method, **params):
    params["_meta"] = {"io.modelcontextprotocol/protocolVersion": PROTOCOL,
                       "io.modelcontextprotocol/clientCapabilities": {},
                       "io.modelcontextprotocol/clientInfo": {"name": "predecessor-return", "version": "1"}}
    headers = {**IDENTITY, "Accept": "application/json, text/event-stream",
               "Mcp-Protocol-Version": PROTOCOL, "Mcp-Method": method}
    if "name" in params:
        headers["Mcp-Name"] = params["name"]
    rpc_id = str(uuid4())
    reply = request(8080, "/mcp", {"jsonrpc": "2.0", "id": rpc_id, "method": method,
                                    "params": params}, headers)
    assert reply["id"] == rpc_id and reply["jsonrpc"] == "2.0" and "error" not in reply, method
    return reply["result"]


def tool(name, **arguments):
    result = rpc("tools/call", name=name, arguments=arguments)
    assert not result.get("isError"), (name, result["content"])
    return json.loads(result["content"][0]["text"])


def discovery(phase):
    return tool("semantic.discovery.request", requestedArtifactType="PROPERTY", intent=f"ci {phase}",
                preferredLanguages=["it"], idempotencyKey=f"d080-discovery-key-{phase}")


def seed(phase):
    asset, profile = str(uuid4()), str(uuid4())
    view = tool("source.analysis.plan", assetId=asset, profileId=profile, expectedRevision=0,
                work=[{"tool": "semantic.search", "arguments": {"q": "tipologia", "type": "PROPERTY"},
                       "columns": [0]}])
    assert view["profile"]["assetId"] == asset and view["profile"]["profileId"] == profile
    assert view["revision"] == 1 and len(view["tasks"]) == view["budget"]["plannedCalls"] == 1
    receipt = discovery(phase)
    assert receipt["status"] == "PENDING" and UUID(receipt["requestId"]).int
    # Only observations are kept outside PostgreSQL, for comparison after return.
    Path(f"/tmp/{phase}.json").write_text(json.dumps({"view": view, "requestId": receipt["requestId"]}))


def replay(phase, original):
    result = discovery(phase)
    assert result["replay"] is True and result["attemptState"] == "SUCCEEDED"
    assert UUID(result["attemptId"]).int and result["resultRef"] == original
    assert result["readArguments"] == {"requestId": original}


# An analysis is bound to the capability manifest that planned it (manifest checksum fence).
# A version reading an analysis planned under a different manifest must refuse it with this
# stable, non-retryable code and leave it untouched; it never returns a changed view.
FENCED = {"code": "ANALYSIS_NOT_FOUND_OR_SCOPE_CHANGED", "retryable": False}


def read_analysis(analysis_id, read_by):
    result = rpc("tools/call", name="source.analysis.read", arguments={"analysisId": analysis_id})
    payload = json.loads(result["content"][0]["text"])
    if read_by == "candidate":
        structured = dict(result["structuredContent"])
        step = structured.pop("next")
        assert step["contract"] == "ouf.next-step.v1"
        # task-50p: an error result also carries the same next block in its text for text-only clients.
        if result.get("isError") and "next" in payload:
            payload = dict(payload)
            assert payload.pop("next") == step, "text and structured next differ"
        assert structured == payload, "candidate changed the owner view or refusal"
    return bool(result.get("isError")), payload


def verify_analysis(written_by, read_by, own):
    saved = json.loads(Path(f"/tmp/{written_by}.json").read_text())
    refused, view = read_analysis(saved["view"]["analysisId"], read_by)
    if not refused:
        assert view == saved["view"], f"changed {written_by} analysis on {read_by}"
        outcome = "identical"
    else:
        assert not own, f"{read_by} refused its own analysis: {view}"
        assert view == FENCED, f"unexpected refusal of {written_by} analysis on {read_by}: {view}"
        outcome = "fenced (planned under another manifest)"
    print(f"Analysis written by {written_by}, read by {read_by}: {outcome}", flush=True)
    return saved


def verify_return():
    names = {entry["name"] for entry in rpc("tools/list")["tools"]}
    assert {"source.analysis.read", "semantic.discovery.request"} <= names
    status = request(8080, "/api/internal/v1/mcp/operations/status", {},
                     {**IDENTITY, "X-OUF-Authorization-Decision-Ref": "d080:1:ouf.system.status"})
    assert status["module"] == "MCP" and status["status"] == "HEALTHY" and status["partial"] is False
    for phase in ("predecessor", "candidate"):
        saved = verify_analysis(phase, "predecessor-return", own=phase == "predecessor")
        replay(phase, saved["requestId"])
    receipt = discovery("return")
    assert receipt["status"] == "PENDING"
    replay("return", receipt["requestId"])
    assert request(8081, "/dispatches") == {
        f"d080-discovery-key-{phase}": 1 for phase in ("predecessor", "candidate", "return")
    }, "unexpected owner dispatch count"


if __name__ == "__main__":
    match sys.argv[1:]:
        case ["wait-owner"] | ["wait-ready"]:
            wait_ready(sys.argv[1] == "wait-owner")
        case ["seed", phase] if phase in ("predecessor", "candidate"):
            seed(phase)
        case ["read-predecessor"]:
            verify_analysis("predecessor", "candidate", own=False)
        case ["return"]:
            verify_return()
        case _:
            sys.exit("unknown probe")
