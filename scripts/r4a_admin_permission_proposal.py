#!/usr/bin/env python3
"""Submit one preplanned Authorization permission proposal through public MCP.

This client authenticates a HUMAN administrator with the ouf-human-admin device
flow, validates the resulting token context locally, verifies MCP discovery, and
calls authorization.permissions.propose through the public Gateway /mcp
endpoint. It cannot confirm or publish the proposal.
"""
import argparse
import base64
import json
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ISSUER="https://auth.ouf-lab.it/realms/ouf"
MCP_URL="https://api.ouf-lab.it/mcp"
PROTOCOL="2026-07-28"
ADMIN_SUB="b93d8cf6-cd14-4ee6-91d7-84cd76c4f500"
REQUIRED_SCOPES={"mcp.connect","authorization.permissions.propose"}
IDEMPOTENCY_RE=re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def jwt_claims(token):
    try:
        part=token.split(".")[1]
        return json.loads(base64.urlsafe_b64decode(part+"="*(-len(part)%4)))
    except Exception as exc:
        raise ValueError("invalid access token payload") from exc


def audience_has(aud,wanted):
    return aud==wanted or (isinstance(aud,list) and wanted in aud)


def validate_admin_claims(claims):
    scopes=set(str(claims.get("scope","")).split())
    if claims.get("iss")!=ISSUER:
        raise ValueError("unexpected issuer")
    if not audience_has(claims.get("aud"),"ouf-api-gateway"):
        raise ValueError("gateway audience missing")
    if claims.get("sub")!=ADMIN_SUB:
        raise ValueError("unexpected administrator subject")
    if claims.get("tenant_id")!="ouf-lab":
        raise ValueError("unexpected tenant")
    if claims.get("ouf_actor_type")!="HUMAN":
        raise ValueError("unexpected actor type")
    missing=sorted(REQUIRED_SCOPES-scopes)
    if missing:
        raise ValueError("missing token scopes: "+",".join(missing))
    if not claims.get("acr"):
        raise ValueError("authentication context missing")
    return True


def oidc_post(url,values):
    req=urllib.request.Request(
        url,
        data=urllib.parse.urlencode(values).encode("ascii"),
        headers={"Content-Type":"application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req,timeout=20) as response:
            return response.status,json.load(response)
    except urllib.error.HTTPError as exc:
        try:return exc.code,json.load(exc)
        except (ValueError,UnicodeError):return exc.code,{}


def device_login():
    with urllib.request.urlopen(ISSUER+"/.well-known/openid-configuration",timeout=20) as response:
        metadata=json.load(response)
    if metadata.get("issuer")!=ISSUER:
        raise ValueError("unexpected OIDC issuer")
    device=metadata.get("device_authorization_endpoint")
    token_endpoint=metadata.get("token_endpoint")
    if not all(isinstance(x,str) and x.startswith(ISSUER+"/protocol/openid-connect/") for x in (device,token_endpoint)):
        raise ValueError("unexpected OIDC endpoints")
    requested="openid authorization.policy.admin mcp.connect authorization.permissions.propose authorization.proposal.read"
    status,start=oidc_post(device,{"client_id":"ouf-human-admin","scope":requested})
    if status!=200 or not all(start.get(k) for k in ("device_code","user_code","verification_uri","expires_in")):
        raise RuntimeError("DEVICE_AUTHORIZATION_FAILED")
    print("OPEN_IN_BROWSER="+str(start["verification_uri"]),flush=True)
    print("ENTER_DEVICE_CODE="+str(start["user_code"]),flush=True)
    interval=max(5,min(30,int(start.get("interval",5))))
    deadline=time.monotonic()+min(600,int(start["expires_in"]))
    while time.monotonic()<deadline:
        time.sleep(interval)
        status,result=oidc_post(token_endpoint,{
            "client_id":"ouf-human-admin",
            "grant_type":"urn:ietf:params:oauth:grant-type:device_code",
            "device_code":start["device_code"],
        })
        if status==200:
            token=result.get("access_token")
            if not isinstance(token,str) or not token:
                raise RuntimeError("TOKEN_MISSING")
            validate_admin_claims(jwt_claims(token))
            return token
        error=result.get("error")
        if error=="slow_down":interval=min(30,interval+5)
        elif error!="authorization_pending":raise RuntimeError("DEVICE_LOGIN_NOT_COMPLETED")
    raise RuntimeError("DEVICE_LOGIN_EXPIRED")


def meta():
    return {
        "io.modelcontextprotocol/protocolVersion":PROTOCOL,
        "io.modelcontextprotocol/clientCapabilities":{},
        "io.modelcontextprotocol/clientInfo":{"name":"ouf-admin-r4a","version":"1"},
    }


def rpc(token,method,params,request_id,idempotency_key=None):
    body={"jsonrpc":"2.0","id":request_id,"method":method,"params":dict(params)}
    body["params"]["_meta"]=meta()
    headers={
        "Authorization":"Bearer "+token,
        "Content-Type":"application/json",
        "Accept":"application/json, text/event-stream",
        "Mcp-Protocol-Version":PROTOCOL,
        "Mcp-Method":method,
    }
    if idempotency_key is not None:
        headers["Idempotency-Key"]=idempotency_key
    req=urllib.request.Request(
        MCP_URL,
        data=json.dumps(body,separators=(",",":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req,timeout=30) as response:
            value=json.load(response)
    except urllib.error.HTTPError as exc:
        try: detail=json.load(exc)
        except Exception: detail={}
        raise RuntimeError("MCP_HTTP_"+str(exc.code)+":"+str(detail.get("error") or detail)) from None
    if value.get("error"):
        err=value["error"]
        raise RuntimeError("MCP_RPC_"+str(err.get("code"))+":"+str(err.get("message")))
    return value.get("result") or {}


def verify_tool(result,name):
    tools=result.get("tools")
    if not isinstance(tools,list):
        raise ValueError("tools/list result missing")
    for tool in tools:
        if isinstance(tool,dict) and tool.get("name")==name:
            return True
    raise ValueError("required MCP tool unavailable: "+name)


def load_request(path):
    value=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value,dict) or set(value)!={"toolName","arguments","expectedBasePolicyRef"}:
        raise ValueError("invalid proposal request file")
    if value["toolName"]!="authorization.permissions.propose":
        raise ValueError("unexpected toolName")
    args=value["arguments"]
    if not isinstance(args,dict) or args.get("operation")!="UPSERT" or not args.get("grantId") or not isinstance(args.get("grant"),dict):
        raise ValueError("invalid proposal arguments")
    return value


def proposal_receipt(result):
    if result.get("isError") is True:
        raise RuntimeError("MCP_TOOL_ERROR")
    content=result.get("content")
    if not isinstance(content,list) or len(content)!=1 or not isinstance(content[0],dict):
        raise ValueError("invalid MCP tool result")
    text=content[0].get("text")
    if not isinstance(text,str):
        raise ValueError("proposal receipt text missing")
    try:value=json.loads(text)
    except json.JSONDecodeError as exc:raise ValueError("proposal receipt is not JSON") from exc
    required=("proposalId","revision","state","expiresAt","approvalPath")
    if not isinstance(value,dict) or not all(value.get(k) is not None for k in required):
        raise ValueError("incomplete proposal receipt")
    if value.get("state")!="PENDING":
        raise ValueError("proposal is not pending")
    return value


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--request-file",type=Path,required=True)
    p.add_argument("--idempotency-key",required=True)
    p.add_argument("--device-login",action="store_true")
    p.add_argument("--submit",action="store_true")
    args=p.parse_args()
    if not IDEMPOTENCY_RE.fullmatch(args.idempotency_key):
        raise ValueError("invalid idempotency key")
    request=load_request(args.request_file)
    if not args.submit:
        print("TOOL_NAME="+request["toolName"])
        print("EXPECTED_BASE_POLICY_REF="+str(request["expectedBasePolicyRef"]))
        print("GRANT_ID="+str(request["arguments"]["grantId"]))
        print("NO_NETWORK_CALL=true")
        print("NO_PROPOSAL_CREATED=true")
        return
    if not args.device_login:
        raise ValueError("--device-login required for --submit")
    token=device_login()
    discovery=rpc(token,"tools/list",{},1)
    verify_tool(discovery,"authorization.permissions.propose")
    print("MCP_TOOL_AVAILABLE=true")
    result=rpc(
        token,
        "tools/call",
        {"name":"authorization.permissions.propose","arguments":request["arguments"]},
        2,
        args.idempotency_key,
    )
    receipt=proposal_receipt(result)
    print("PROPOSAL_ID="+str(receipt["proposalId"]))
    print("PROPOSAL_REVISION="+str(receipt["revision"]))
    print("PROPOSAL_STATE="+str(receipt["state"]))
    print("PROPOSAL_EXPIRES_AT="+str(receipt["expiresAt"]))
    print("APPROVAL_PATH="+str(receipt["approvalPath"]))
    print("ACTIVE_NOT_PUBLISHED=true")
    print("THS_CONFIRMATION_REQUIRED=true")


if __name__=="__main__":
    try:main()
    except (OSError,UnicodeError,ValueError,RuntimeError,json.JSONDecodeError,KeyError) as exc:
        print("ADMIN_MCP_PROPOSAL_BLOCKED="+str(exc),file=sys.stderr)
        raise SystemExit(1) from None
