"""Regenerate the self-contained D-084 $defs in the shipped capability manifest."""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/ouf-value-types-v1.json"
MANIFEST = ROOT / "internal/manifest/capabilities.json"
DIGEST = "c046646c3030f1bcf4460741bbd09351f9bb501cb054d8ed45ce2e7d98dd4247"
NAME = {"type": "string", "pattern": "^[a-z][A-Za-z0-9]{0,63}$"}
# publicationSetId is the UUID used by semantic.get and by Onboarding.
PIN = {"type": "string", "pattern": (
    r"^[A-Za-z][A-Za-z0-9+.-]*:[^\s@]+@[A-Fa-f0-9]{8}(-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}$"
)}


def ref(name):
    return {"$ref": "#/$defs/" + name}


def obj(properties, required=()):
    schema = {"type": "object", "additionalProperties": False, "properties": properties}
    if required:
        schema["required"] = list(required)
    return schema


def array(items, minimum, maximum):
    return {"type": "array", "items": items, "minItems": minimum, "maxItems": maximum}


def conditional(field, values, rule):
    return {
        "if": {"required": [field], "properties": {field: {"enum": values}}},
        "then": rule,
    }


def ids(entries):
    return {"enum": [entry["id"] for entry in entries]}


def datatype_defs(contract):
    text = {"type": "string", "minLength": 1, "maxLength": 1024}
    # These four base types have lexical canonical values. Semantic's
    # DATATYPE fixtures use those strings for inclusive bounds as well.
    bounds = {"minInclusive": text, "maxInclusive": text}
    component = obj({
        "name": NAME,
        "type": {"oneOf": [ids(contract["baseTypes"]), {**PIN, "maxLength": 1024}]},
        "cardinality": {"enum": ["ONE", "MANY"]},
        "required": {"type": "boolean"},
        "labels": {"$ref": "#/properties/labels"},
        "constraints": obj({**bounds, "maxLength": {"type": "integer", "minimum": 0},
                            "vocabularyRef": text}),
    }, ["name", "type", "cardinality", "required"])
    # Enforce the closed sets of constraint keys. Comparing bounds, resolving
    # the type graph and checking distinct component names belong to Semantic.
    component["allOf"] = [
        conditional("type", types, {"properties": {"constraints": {
            "propertyNames": {"enum": keys}
        }}})
        for types, keys in [
            (["integer", "decimal", "date", "datetime"], list(bounds)),
            (["string"], ["maxLength"]),
            (["concept"], ["vocabularyRef"]),
        ]
    ]
    component["allOf"].append({
        "if": {"properties": {"type": {"anyOf": [PIN, {"enum": ["boolean", "geometry"]}]}}},
        "then": {"properties": {"constraints": {"maxProperties": 0}}},
    })
    return {"datatypeComponents": {
        **array(component, 1, 32),
        "uniqueItems": True,
        "description": "Use unique component names. The Semantic Registry resolves published pins and checks cycles, depth (4) and total leaf components (256).",
    }}


def reader_rules(readers, selector, alias=False):
    rules = []
    for reader in readers:
        if not reader.get("params"):
            continue
        required = list(reader["params"])
        rule = {"required": ["transformParams"],
                "properties": {"transformParams": {"required": required}}}
        if alias and required == ["valueMapRef"]:
            rule = {"anyOf": [rule, {"required": ["valueMapRef"],
                                    "properties": {"valueMapRef": {"minLength": 1}}}]}
        rules.append(conditional(selector, [reader["id"]], rule))
    return {"allOf": rules}


def mapping_defs(contract, fields):
    readers = contract["readers"]
    constructors = contract["constructors"]
    field = fields["items"]["properties"]
    params = {
        "crs": {"type": "string", "pattern": "^EPSG:[0-9]+$", "maxLength": 512},
        "valueMapRef": {**field["valueMapRef"], "minLength": 1},
        "separator": {"type": "string", "minLength": 1, "maxLength": 8},
        "itemReader": ref("reader"),
    }
    source = {"fieldName": field["fieldName"], "reader": ref("reader"),
              "transformParams": ref("transformParams")}
    input_schema = obj({"role": {"type": "string", "minLength": 1, "maxLength": 64}, **source},
                       ["role", "fieldName", "reader"])
    leaf = obj({"component": NAME, **source}, ["component", "fieldName", "reader"])
    for schema in (input_schema, leaf):
        schema.update(reader_rules(readers, "reader"))

    # Each constructor's arity, role vocabulary and required parameters come
    # from its own contract entry. Type compatibility is resolved by Onboarding.
    variants = []
    for ctor in constructors:
        inputs = ctor["inputs"]
        keys = ctor.get("params", {})
        input_rules = {
            "minItems": sum(i.get("min", 1) for i in inputs),
            "maxItems": sum(i.get("max", 1) for i in inputs),
            "items": {"properties": {"role": {"enum": [i["role"] for i in inputs]}}},
        }
        if len(inputs) > 1:
            input_rules["allOf"] = [
                {"contains": {"properties": {"role": {"const": i["role"]}}},
                 "minContains": i.get("min", 1), "maxContains": i.get("max", 1)}
                for i in inputs
            ]
        variants.append(conditional("constructorId", [ctor["id"]], {"properties": {
            "inputs": input_rules,
            "params": obj({k: params[k] for k in keys}, keys),
        }}))
    ctor_fields = {"constructorId": ids(constructors),
                   "inputs": {"type": "array", "items": ref("constructorInput")},
                   "params": {"type": "object"}}
    constructor = obj(ctor_fields, ctor_fields)
    constructor["allOf"] = [ref("constructorRules")]
    target = {k: field[k] for k in ("targetPropertyIri", "dataAccessLabel")}
    ctor_mapping = obj({"kind": {"const": "CONSTRUCTOR"}, **target, **ctor_fields},
                       ["kind", *target, *ctor_fields])
    ctor_mapping["allOf"] = [ref("constructorRules")]

    defs = {
        "reader": ids(readers), "transformParams": obj(params),
        "fieldReaderParams": reader_rules(readers, "transform", alias=True),
        "constructorInput": input_schema, "constructor": constructor,
        "constructorRules": {"allOf": variants},
        "fieldComponent": leaf,
        "constructorComponent": obj({"component": NAME, "constructor": ref("constructor")},
                                    ["component", "constructor"]),
    }
    # Unroll just the references. Each level reuses both kinds of leaf and
    # a level-four structured object has no further structured alternative.
    for depth in range(4, 0, -1):
        alternatives = [ref("fieldComponent"), ref("constructorComponent")]
        if depth < 4:
            alternatives.append(obj({"component": NAME, "structured": ref(f"structured{depth+1}")},
                                    ["component", "structured"]))
        structured = {"typeRef": {**PIN, "maxLength": 512},
                      "components": {**array({"oneOf": alternatives}, 1, 32), "uniqueItems": True}}
        if depth == 1:
            fields_with_target = {"kind": {"const": "STRUCTURED"}, **target, **structured}
            structured_mapping = obj(fields_with_target, fields_with_target)
        else:
            defs[f"structured{depth}"] = obj(structured, structured)
    defs["compositions"] = array({"oneOf": [ctor_mapping, structured_mapping]}, 0, fields["maxItems"])
    defs["compositions"]["description"] = (
        "Use INCLUDE fields from this file. Onboarding resolves pinned DATATYPEs and checks "
        "component names, output types and the limit of 256 leaves per structured value."
    )
    return defs


def generate(source, contract):
    tools = {c["toolName"]: c for c in json.loads(source)["capabilities"]}
    fields = tools["source.onboarding.create"]["inputSchema"]["properties"]["fields"]
    mapping = mapping_defs(contract, fields)
    for name, defs in [("semantic.propose", datatype_defs(contract)),
                       ("source.onboarding.create", mapping),
                       ("source.onboarding.revision.prepare", mapping)]:
        # Replace only this input schema's generated $defs, preserving the
        # surrounding hand-maintained manifest verbatim.
        start = source.index('        "$defs": ', source.index(f'"toolName": "{name}"'))
        start += len('        "$defs": ')
        previous, length = json.JSONDecoder().raw_decode(source[start:])
        assert previous == tools[name]["inputSchema"]["$defs"]
        encoded = format_schema(defs).replace("\n", "\n        ")
        source = source[:start] + encoded + source[start + length:]
    return source


def format_schema(value, indent=0):
    """Keep short property schemas on one line in the generated artifact."""
    compact = json.dumps(value)
    if len(compact) + indent <= 96 or not isinstance(value, (dict, list)):
        return compact
    if isinstance(value, dict):
        left, right = "{", "}"
        parts = [json.dumps(k) + ": " + format_schema(v, indent + 2) for k, v in value.items()]
    else:
        left, right = "[", "]"
        parts = [format_schema(v, indent + 2) for v in value]
    prefix = "\n" + " " * (indent + 2)
    return left + prefix + ("," + prefix).join(parts) + "\n" + " " * indent + right


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    options = parser.parse_args()
    data = CONTRACT.read_bytes()
    if hashlib.sha256(data).hexdigest() != DIGEST:
        raise SystemExit("VALUE_TYPES_CONTRACT_DIGEST_MISMATCH")
    before = MANIFEST.read_text(encoding="utf-8")
    after = generate(before, json.loads(data))
    if before != after:
        if options.check:
            raise SystemExit("VALUE_TYPES_MANIFEST_OUT_OF_DATE")
        MANIFEST.write_text(after, encoding="utf-8")
