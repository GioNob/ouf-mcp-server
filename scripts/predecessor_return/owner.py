"""Loopback-only Authorization and Gateway fixtures for the D-080 database test."""

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from uuid import uuid4


def encode(value):
    return json.dumps(value, separators=(",", ":")).encode()


def policy():
    now = datetime.now(timezone.utc)
    capabilities = {
        "ouf.managed-source.preview": "READ",
        "ouf.semantic.search": "READ",
        "ouf.semantic.discovery": "COMMAND",
        "ouf.system.status": "READ",
    }
    bundle = {
        "bundleId": "d080", "version": 1, "publishedAt": now.isoformat(),
        "capabilities": [
            {"capabilityId": cap, "operation": op, "requiredScope": cap, "allowedActors": ["HUMAN"]}
            for cap, op in capabilities.items()
        ],
        "grants": [
            {"grantId": cap, "capabilityId": cap, "tenantId": "ci", "subjectId": "analyst",
             "validFrom": now.isoformat(), "validUntil": (now + timedelta(hours=1)).isoformat()}
            for cap in capabilities
        ],
    }
    return {"bundleId": "d080", "bundleVersion": 1, "activatedAt": now.isoformat(),
            "bundle": bundle, "contentHash": sha256(encode(bundle)).hexdigest()}


class Owner(BaseHTTPRequestHandler):
    bundle = policy()
    # Keep the dispatch counter alive through both changes of MCP process.
    dispatches = {}

    def reply(self, value, status=200):
        raw = encode(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/dispatches":
            self.reply(self.dispatches)
        elif self.path == "/bundle" and self.headers.get("Authorization") == "Bearer d080-fixture":
            self.reply(self.bundle)
        else:
            self.send_error(404)

    def do_POST(self):
        if (self.headers.get("Authorization") != "Bearer d080-fixture"
                or self.headers.get("X-OUF-Delegation") != "d080-fixture"):
            self.send_error(403)
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        args = request["Arguments"]
        if self.path == "/execute/managed.file/preview" and args["view"] == "COLUMNS":
            self.reply({"assetId": args["assetId"], "profileId": args["profileId"],
                        "profileVersion": 1, "schemaHash": "sha256:" + "a" * 64,
                        "columnCount": 1, "view": "COLUMNS", "offset": 0, "hasMore": False,
                        "columns": [{"name": "tipologia"}]})
        elif self.path == "/execute/semantic/search":
            self.reply({"items": []})
        elif self.path == "/execute/semantic/discovery/request":
            key = args["idempotencyKey"]
            if self.headers.get("Idempotency-Key") != key:
                self.send_error(400)
                return
            self.dispatches[key] = self.dispatches.get(key, 0) + 1
            self.reply({"requestId": str(uuid4()), "status": "PENDING"})
        else:
            self.send_error(404)


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 8081), Owner).serve_forever()
