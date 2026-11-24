"""Regenerate the self-contained D-084 $defs in the shipped capability manifest."""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/ouf-value-types-v1.json"
VALUE_MAP_CONTRACT = ROOT / "contracts/ouf-value-maps-v1.json"
MANIFEST = ROOT / "internal/manifest/capabilities.json"
DIGEST = "3b85052e253b985f1082527eb595d2f02e5336047203deb3503a9766c1324295"
VALUE_MAP_DIGEST = "40b247ad3422e09245a8314fdcc59ffe0b039e25733a2ca8f31aa6ba03ff2d2e"
NAME = {"type": "string", "pattern": "^[a-z][A-Za-z0-9]{0,63}$"}
# publicationSetId is the UUID used by semantic.get and by Onboarding.
PUBLICATION_SET_ID = r"[A-Fa-f0-9]{8}(-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}"
PIN = {"type": "string", "pattern": (
    r"^[A-Za-z][A-Za-z0-9+.-]*:[^\s@]+@" + PUBLICATION_SET_ID + "$"
)}
ABSOLUTE_IRI = r'^[A-Za-z][A-Za-z0-9+.-]*:([^\s\x00-\x1F\x7F<>\"{}`|\\^%]|%[A-Fa-f0-9]{2})+'


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


def value_map_defs(labels):
    # Explicit Unicode White_Space from ouf.value-types.v1 r2.
    whitespace = "\u0009-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000"
    space_or_control = {"not": {"pattern": r"[\x00-\x1F\x7F]|[" + whitespace + "]"}}
    code = {
        "type": "string",
        "minLength": 1,
        "maxLength": 128,
        "not": {"pattern": r"[\x00-\x1F\x7F-\x9F]|^[" + whitespace + "]|[" + whitespace + "]$"},
    }
    entry = obj({"code": code, "concept": {**ref("valueMapIri"), "maxLength": 512}}, ["code", "concept"])
    return {
        "valueMapIri": {"type": "string", "format": "iri", **space_or_control, "pattern": ABSOLUTE_IRI + "$"},
        "valueMapRationale": obj({"sourceColumn": obj({
            "fieldName": {"type": "string", "minLength": 1, "maxLength": 256},
            "sourceId": {"type": "string"},
        }, ["fieldName"])}, ["sourceColumn"]),
        "valueMapDefinition": obj({
            "targetVocabulary": {"type": "string", **space_or_control,
                                 "pattern": ABSOLUTE_IRI + "@" + PUBLICATION_SET_ID + "$"},
            "entries": {**array(entry, 1, 2000),
                        "description": "Codes must be unique and are case sensitive. Concepts may repeat."},
            "labels": obj({key: labels[key] for key in ("it", "en")}),
        }, ["targetVocabulary", "entries"]),
    }


def datatype_defs(contract):
    text = {"type": "string", "minLength": 1, "maxLength": 1024}
    # Format is an annotation in JSON Schema 2020-12 by default. Check the
    # proleptic Gregorian calendar even on validators that ignore annotations.
    leap_year = r"([0-9]{2}(0[48]|[2468][048]|[13579][26])|(0[048]|[2468][048]|[13579][26])00)"
    ordinary = (r"[0-9]{4}-((0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-8])"
                r"|(0[13-9]|1[0-2])-(29|30)|(0[13578]|1[02])-31)")
    date = "(" + ordinary + "|" + leap_year + "-02-29)"
    bound_forms = {
        "integer": {"pattern": r"^(0|-?[1-9][0-9]*)$"},
        "decimal": {"pattern": r"^(0|-?[1-9][0-9]*|-?(0|[1-9][0-9]*)\.[0-9]*[1-9])$"},
        "date": {"format": "date", "pattern": "^" + date + "$"},
        "datetime": {"format": "date-time", "pattern": (
            "^" + date + r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,9})?Z$"
        )},
    }
    bounds = {"minInclusive": text, "maxInclusive": text}
    component = obj({
        "name": NAME,
        "type": {"oneOf": [ids(contract["baseTypes"]), {**PIN, "maxLength": 1024}]},
        "cardinality": {"enum": ["ONE", "MANY"]},
        "required": {"type": "boolean"},
        "labels": {"$ref": "#/properties/labels"},
        "constraints": obj({**bounds, "maxLength": {"type": "integer", "minimum": 0},
                            "vocabularyRef": {**text, "format": "iri", "pattern": ABSOLUTE_IRI + "$"}}),
    }, ["name", "type", "cardinality", "required"])
    # Enforce the closed sets of constraint keys. Comparing bounds, resolving
    # the type graph and checking distinct component names belong to Semantic.
    component["allOf"] = [
        conditional("type", types, {"properties": {"constraints": {
            "propertyNames": {"enum": keys}
        }}})
        for types, keys in [
            (["string"], ["maxLength"]),
            (["concept"], ["vocabularyRef"]),
        ]
    ]
    for kind, form in bound_forms.items():
        component["allOf"].append(conditional("type", [kind], {"properties": {"constraints": {
            "propertyNames": {"enum": list(bounds)},
            "properties": {key: form for key in bounds},
        }}}))
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
    field_item = obj(source, ["fieldName", "reader"])
    for schema in (input_schema, leaf, field_item):
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
        param_types = {**params, "itemReaderParams": {"type": "object"}}
        ctor_params = obj({k: param_types[k] for k in keys},
                          [k for k, rule in keys.items() if rule.startswith("required")])
        if "itemReaderParams" in keys:
            plain = [reader["id"] for reader in readers if not reader.get("params")]
            ctor_params["allOf"] = [conditional("itemReader", plain, {
                "properties": {"itemReaderParams": obj({})},
            })]
            for reader in readers:
                reader_keys = reader.get("params", {})
                if reader_keys:
                    ctor_params["allOf"].append(conditional("itemReader", [reader["id"]], {
                        "required": ["itemReaderParams"],
                        "properties": {"itemReaderParams": obj({k: params[k] for k in reader_keys},
                                                               reader_keys)},
                    }))
        variants.append(conditional("constructorId", [ctor["id"]], {"properties": {
            "inputs": input_rules,
            "params": ctor_params,
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
        "fieldItem": field_item,
        "constructorItem": obj({"constructor": {
            "$ref": "#/$defs/constructor",
            "properties": {"constructorId": {"enum": [
                ctor["id"] for ctor in constructors if ctor["output"] != "list"
            ]}},
        }}, ["constructor"]),
        "constructorComponent": obj({"component": NAME, "constructor": ref("constructor")},
                                    ["component", "constructor"]),
    }
    # Unroll just the references. Each level reuses both kinds of leaf and
    # a level-four structured object has no further structured alternative.
    for depth in range(4, 0, -1):
        alternatives = [ref("fieldComponent"), ref("constructorComponent")]
        item_builders = [ref("fieldItem"), ref("constructorItem")]
        if depth < 4:
            item_builders.append(obj({"structured": ref(f"structured{depth+1}")}, ["structured"]))
            alternatives.append(obj({"component": NAME, "structured": ref(f"structured{depth+1}")},
                                    ["component", "structured"]))
        alternatives.append(obj({"component": NAME, "items": {
            "type": "array", "items": {"oneOf": item_builders},
        }}, ["component", "items"]))
        structured = {"typeRef": {**PIN, "maxLength": 512},
                      "components": {**array({"oneOf": alternatives}, 1, 32), "uniqueItems": True}}
        if depth == 1:
            fields_with_target = {"kind": {"const": "STRUCTURED"}, **target, **structured}
            structured_mapping = obj(fields_with_target, fields_with_target)
        else:
            defs[f"structured{depth}"] = obj(structured, structured)
    defs["compositions"] = array({"oneOf": [ctor_mapping, structured_mapping]}, 0, fields["maxItems"])
    defs["compositions"]["description"] = (
        "Use INCLUDE fields from this file. Build MANY with ordered items or LIST_SPLIT_V1. "
        "Onboarding checks component cardinality, output types and the limit of 256 leaves. "
        "New preparations require ACTIVE DATATYPE pins at every level. Published bundles may "
        "resolve ACTIVE, DEPRECATED or RETIRED pins. Concept membership requires a pinned CONCEPT "
        "of the vocabulary or a pinned value map targeting it."
    )
    return defs


UNIT_DEFS = ("uuid", "unitRef", "unitProfileRef")


def generate(source, contract):
    tools = {c["toolName"]: c for c in json.loads(source)["capabilities"] if "toolName" in c}
    fields = tools["source.onboarding.create"]["inputSchema"]["properties"]["fields"]
    mapping = mapping_defs(contract, fields)
    proposal_labels = tools["semantic.propose"]["inputSchema"]["properties"]["labels"]["properties"]
    semantic = {**datatype_defs(contract), **value_map_defs(proposal_labels)}
    for name, defs in [("semantic.propose", semantic),
                       ("source.onboarding.create", mapping),
                       ("source.onboarding.revision.prepare", mapping)]:
        # Replace only this input schema's generated $defs, preserving the
        # surrounding hand-maintained manifest verbatim.
        start = source.index('        "$defs": ', source.index(f'"toolName": "{name}"'))
        start += len('        "$defs": ')
        previous, length = json.JSONDecoder().raw_decode(source[start:])
        assert previous == tools[name]["inputSchema"]["$defs"]
        # Hand-maintained $defs of ouf.extraction-unit.v1 (task-62e) follow the generated ones.
        extra = {k: v for k, v in previous.items() if k in UNIT_DEFS and k not in defs}
        encoded = format_schema({**defs, **extra}).replace("\n", "\n        ")
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
    if hashlib.sha256(VALUE_MAP_CONTRACT.read_bytes()).hexdigest() != VALUE_MAP_DIGEST:
        raise SystemExit("VALUE_MAPS_CONTRACT_DIGEST_MISMATCH")
    before = MANIFEST.read_text(encoding="utf-8")
    after = generate(before, json.loads(data))
    if before != after:
        if options.check:
            raise SystemExit("VALUE_TYPES_MANIFEST_OUT_OF_DATE")
        MANIFEST.write_text(after, encoding="utf-8")
