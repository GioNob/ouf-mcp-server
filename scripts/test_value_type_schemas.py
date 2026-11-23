import hashlib
import json
import os
import unittest
from copy import deepcopy
from pathlib import Path

from jsonschema import Draft202012Validator

from scripts.generate_value_type_schemas import (
    CONTRACT, DIGEST, MANIFEST, VALUE_MAP_CONTRACT, VALUE_MAP_DIGEST, generate,
)


PIN = "urn:example:address@00000000-0000-4000-8000-000000000001"
UUID = "00000000-0000-4000-8000-000000000002"
CREATE = "source.onboarding.create"
REVISION = "source.onboarding.revision.prepare"
SEMANTIC = "semantic.propose"
FIXTURES = MANIFEST.parents[1] / "kernel/testdata/value-types"
GATEWAY_SCHEMAS = {
    SEMANTIC: "mcp-gateway-semantic-proposals-v1.json",
    CREATE: "mcp-gateway-managed-file-dispatch-v1.json",
    REVISION: "mcp-gateway-source-revision-prepare-dispatch-v1.json",
}


def dispatch_cases():
    for tool in GATEWAY_SCHEMAS:
        request = json.loads((FIXTURES / (tool + ".json")).read_bytes())
        invalid = deepcopy(request)
        args = invalid["Arguments"]
        if tool == SEMANTIC:
            price = next(c for c in args["definition"]["components"] if c["name"] == "price")
            price["constraints"]["minInclusive"] = 1
        elif tool == CREATE:
            split = next(c for c in args["compositions"] if c["kind"] == "CONSTRUCTOR")
            split["params"]["itemReaderParams"] = {"valueMapRef": PIN}
        else:
            args["compositions"][0]["components"][0]["items"] = [{}]
        yield tool, request, invalid
    request = json.loads((FIXTURES / (SEMANTIC + ".json")).read_bytes())
    request["Arguments"].update(artifactType="VALUE_MAP", definition={
        "targetVocabulary": PIN,
        "entries": [{"code": "A", "concept": "urn:concept"}],
    })
    invalid = deepcopy(request)
    invalid["Arguments"]["definition"]["entries"][0]["code"] = "private-source-value "
    yield SEMANTIC, request, invalid
    request = deepcopy(request)
    request["Arguments"]["rationale"] = {"sourceColumn": {"fieldName": "categoria", "sourceId": "places"}}
    invalid = deepcopy(request)
    invalid["Arguments"]["rationale"]["sourceColumn"]["fieldName"] = ""
    yield SEMANTIC, request, invalid


class ValueTypeSchemas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(CONTRACT.read_bytes())
        cls.value_maps = json.loads(VALUE_MAP_CONTRACT.read_bytes())
        cls.tools = {c["toolName"]: c for c in json.loads(MANIFEST.read_bytes())["capabilities"] if "toolName" in c}
        cls.schemas = {}
        for name in (SEMANTIC, CREATE, REVISION, "semantic.search"):
            schema = cls.tools[name]["inputSchema"]
            Draft202012Validator.check_schema(schema)
            cls.schemas[name] = Draft202012Validator(schema)
        cls.proposal = {
            "semanticId": "urn:example:place", "artifactType": "DATATYPE", "namespace": "urn:example:",
            "localName": "place", "ownerRef": "owner", "authorityRef": "authority",
            "semanticVersion": "1.0.0", "labels": {"it": "Luogo"}, "commandKey": "draft-1",
        }
        cls.create = {
            "sourceId": "places", "assetId": UUID, "profileId": UUID, "name": "Places",
            "owner": "Comune", "targetClassIri": "urn:place", "semanticRefs": [PIN],
            "sourceObjectKeyFields": [],
        }
        cls.revision = {
            "sourceId": "places", "assetRef": {"assetId": UUID, "profileId": UUID},
            "parentVersionId": UUID, "expectedVersion": 1, "commandKey": "rev-1",
            "configurationHash": "sha256:" + "a" * 64,
            "publicationEffect": "NEW_PUBLICATION_MAY_START_ACQUISITION",
        }

    def accepts(self, name, args, expected=True):
        self.assertEqual(self.schemas[name].is_valid(args), expected, name)

    def test_contract_and_generated_manifest(self):
        self.assertEqual(hashlib.sha256(CONTRACT.read_bytes()).hexdigest(), DIGEST)
        self.assertEqual(self.contract["revision"], 2)
        source = MANIFEST.read_text(encoding="utf-8")
        self.assertEqual(generate(source, self.contract), source)
        components = self.tools[SEMANTIC]["inputSchema"]["$defs"]["datatypeComponents"]
        self.assertEqual(components["items"]["properties"]["type"]["oneOf"][0]["enum"],
                         [t["id"] for t in self.contract["baseTypes"]])
        for name in (CREATE, REVISION):
            defs = self.tools[name]["inputSchema"]["$defs"]
            self.assertEqual(defs["reader"]["enum"], [r["id"] for r in self.contract["readers"]])
            self.assertEqual(defs["constructor"]["properties"]["constructorId"]["enum"],
                             [c["id"] for c in self.contract["constructors"]])
            self.assertEqual(defs["compositions"]["maxItems"], 1024)
        for name in (CREATE, REVISION, SEMANTIC):
            guidance = " ".join(self.tools[name]["useWhen"])
            for word in ("controlled vocabulary", "DATATYPE", "THS", "publish"):
                self.assertIn(word, guidance)

    def test_datatype(self):
        base = {"name": "value", "cardinality": "ONE", "required": True}
        def check(components, valid=True):
            self.accepts(SEMANTIC, {**self.proposal, "definition": {"components": components}}, valid)
        for entry in self.contract["baseTypes"]:
            check([{**base, "type": entry["id"]}])
        check([{**base, "type": PIN, "cardinality": "MANY", "labels": {"it": "Indirizzi"}}])
        for kind, constraints, valid in [
            ("decimal", {"minInclusive": "-90", "maxInclusive": "90"}, True),
            ("integer", {"minInclusive": "9007199254740993"}, True),
            ("date", {"minInclusive": "2026-01-01"}, True),
            ("date", {"minInclusive": 0}, False),
            ("string", {"maxLength": 64}, True), ("concept", {"vocabularyRef": PIN}, True),
            ("decimal", {"maxLength": 12}, False), ("string", {"minInclusive": "0"}, False),
            ("geometry", {"vocabularyRef": PIN}, False), (PIN, {"unknown": "x"}, False),
        ]:
            with self.subTest(kind=kind, constraints=constraints):
                check([{**base, "type": kind, "constraints": constraints}], valid)
        components = [{**base, "name": f"v{i}", "type": "string"} for i in range(33)]
        check(components[:32])
        check(components[:1] * 2, False)
        check(components, False)
        check([], False)
        for invalid in [{"name": "A"}, {"name": "a" * 65}, {"type": "urn:unpinned"}, {"type": "urn:type@bad"},
                        {"type": "plain@set"}, {"required": "false"}, {"cardinality": "SINGLE"}]:
            check([{**components[0], **invalid}], False)
        for key in base:
            check([{k: v for k, v in components[0].items() if k != key}], False)
        self.accepts(SEMANTIC, {**self.proposal, "definition": {}}, False)
        self.accepts("semantic.search", {"q": "place", "type": "DATATYPE"})

    def test_existing_semantic_artifacts(self):
        for kind in ("CLASS", "PROPERTY", "RELATIONSHIP", "VOCABULARY", "CONCEPT", "ONTOLOGY"):
            args = {**self.proposal, "artifactType": kind}
            self.accepts(SEMANTIC, {**args, "definition": {"description": {"en": "Place"}}})
            self.accepts(SEMANTIC, {**args, "definition": {"components": [
                {"name": "x", "type": "string", "cardinality": "ONE", "required": False}
            ]}}, False)
        turtle = {**self.proposal, "mediaType": "text/turtle", "rdfTurtle": "<urn:a> a <urn:b> ."}
        self.accepts(SEMANTIC, {**turtle, "artifactType": "ONTOLOGY"})
        self.accepts(SEMANTIC, turtle, False)

    def test_value_map_contract_and_guidance(self):
        self.assertEqual(hashlib.sha256(VALUE_MAP_CONTRACT.read_bytes()).hexdigest(), VALUE_MAP_DIGEST)
        self.assertEqual(self.value_maps["revision"], 2)
        self.assertIn(DIGEST, self.value_maps["extends"])
        limits = self.tools[SEMANTIC]["operationalLimits"]
        self.assertEqual(limits["maxRequestBytes"], 65536)
        self.assertEqual(limits["maxValueMapArgumentsBytes"], 1048576)
        self.assertEqual(limits["maxValueMapDefinitionBytes"], 786432)
        for tool, phrases in {
            SEMANTIC: ("column of codes", "controlled vocabulary", "distinctValues", "source.file.profile",
                       "semantic.search", "semantic.get", "only verified concepts", "THS", "publication",
                       "never invent concepts", "rationale.sourceColumn", "distinctValues is absent",
                       "unsuitable", "transform: VALUE_MAP_V1", "transformParams.valueMapRef"),
            "source.file.profile": ("VALUE_MAP", "distinctValues", "source.file.preview", "unsuitable"),
            "source.file.preview": ("distinctValues", "rationale.sourceColumn"),
            "semantic.search": ("published vocabulary", "published concepts", "VALUE_MAP"),
            "semantic.get": ("published vocabulary", "concepts", "VALUE_MAP"),
            CREATE: ("transform: VALUE_MAP_V1", "transformParams.valueMapRef"),
            REVISION: ("transform: VALUE_MAP_V1", "transformParams.valueMapRef"),
        }.items():
            guidance = " ".join(self.tools[tool]["useWhen"])
            for phrase in phrases:
                with self.subTest(tool=tool, phrase=phrase):
                    self.assertIn(phrase, guidance)

    def test_value_map_definition(self):
        proposal = {**self.proposal, "artifactType": "VALUE_MAP"}
        definition = {"targetVocabulary": PIN, "entries": [{"code": "A", "concept": "urn:concept"}]}

        def check(candidate, valid=True):
            self.accepts(SEMANTIC, {**proposal, "definition": candidate}, valid)

        check(definition)
        check({**definition, "labels": {"it": "Codici", "en": "Codes"}})
        entries = [{"code": f"{i:04d}" + "a" * 124, "concept": "urn:" + "z" * 235} for i in range(2001)]
        check({**definition, "entries": entries[:2000]})
        for invalid in (
            {}, {"targetVocabulary": PIN}, {"entries": definition["entries"]},
            {**definition, "targetVocabulary": "urn:vocabulary"},
            {**definition, "targetVocabulary": "relative@" + UUID},
            {**definition, "targetVocabulary": PIN + "\n"},
            {**definition, "targetVocabulary": "urn:a\x00@" + UUID},
            {**definition, "targetVocabulary": "urn:a%xx@" + UUID},
            {**definition, "entries": []}, {**definition, "entries": entries},
            {**definition, "labels": {"de": "Codes"}}, {**definition, "labels": "Codes"},
            {**definition, "labels": {"en": ""}}, {**definition, "description": {}},
        ):
            check(invalid, False)
        for code in ("a b", "A", "a", "\U0001F30D" * 128):
            check({**definition, "entries": [{"code": code, "concept": "urn:c"}]})
        for invalid in (
            {"code": "", "concept": "urn:c"}, {"code": "x" * 129, "concept": "urn:c"},
            {"code": "\U0001F30D" * 129, "concept": "urn:c"}, {"code": 7, "concept": "urn:c"},
            {"code": "A"}, {"concept": "urn:c"}, {"code": "A", "concept": "urn:c", "extra": True},
            {"code": "A", "concept": "relative"}, {"code": "A", "concept": "urn:c\n"},
            {"code": "A", "concept": "urn:a%xx"},
            *({"code": value, "concept": "urn:c"} for value in (" A", "A ", "A\u00a0", "\u2003A", "A\n",
                                                              "A\tB", "A\x00B", "A\x7fB")),
        ):
            check({**definition, "entries": [invalid]}, False)
        for kind in ("DATATYPE", "CONCEPT", "ONTOLOGY"):
            self.accepts(SEMANTIC, {**proposal, "artifactType": kind, "definition": definition}, False)
        self.accepts(SEMANTIC, {**proposal, "definition": definition, "extra": True}, False)
        self.accepts(SEMANTIC, {**proposal, "definition": definition, "semanticId": "relative"}, False)
        self.accepts(SEMANTIC, {**proposal, "rdfTurtle": "<urn:a> a <urn:b> .", "mediaType": "text/turtle"}, False)

    def test_value_map_r2_lengths_and_rationale(self):
        proposal = {**self.proposal, "artifactType": "VALUE_MAP", "definition": {
            "targetVocabulary": PIN, "entries": [{"code": "A", "concept": "urn:" + "é" * 508}],
        }}
        self.accepts(SEMANTIC, proposal)
        for column in ({"fieldName": "categoria"}, {"fieldName": "🌍" * 256, "sourceId": "places"}):
            self.accepts(SEMANTIC, {**proposal, "rationale": {"sourceColumn": column}})
        for rationale in (
            {}, {"note": "text"}, {"sourceColumn": {}}, {"sourceColumn": {"fieldName": ""}},
            {"sourceColumn": {"fieldName": "🌍" * 257}}, {"sourceColumn": {"fieldName": "a", "sourceId": 7}},
            {"sourceColumn": {"fieldName": "a", "sourceValue": "private"}},
        ):
            self.accepts(SEMANTIC, {**proposal, "rationale": rationale}, False)
        proposal["definition"]["rationale"] = {"sourceColumn": {"fieldName": "categoria"}}
        self.accepts(SEMANTIC, proposal, False)
        del proposal["definition"]["rationale"]
        proposal["definition"]["entries"][0]["concept"] += "é"
        self.accepts(SEMANTIC, proposal, False)
        self.accepts(SEMANTIC, {**self.proposal, "artifactType": "CLASS", "definition": {},
                                "rationale": {"sourceColumn": {"fieldName": "categoria"}}}, False)

    def test_value_map_code_uses_contract_whitespace(self):
        entry = {"code": "A", "concept": "urn:c"}
        proposal = {**self.proposal, "artifactType": "VALUE_MAP", "definition": {
            "targetVocabulary": PIN, "entries": [entry],
        }}
        vector, = (v for v in self.contract["whitespace"]["vectors"] if v["reader"] == "VALUE_MAP_V1")
        entry["code"] = vector["cell"]
        self.accepts(SEMANTIC, proposal, False)
        entry["code"] = "A1"
        self.accepts(SEMANTIC, proposal)
        whitespace = {chr(c) for c in (*range(9, 14), 0x20, 0x85, 0xA0, 0x1680,
                                        *range(0x2000, 0x200B), 0x2028, 0x2029, 0x202F, 0x205F, 0x3000)}
        controls = {chr(c) for c in (*range(0x20), *range(0x7F, 0xA0))}
        for char in sorted(whitespace | controls | {"\ufeff", "\u200b", "\u180e"}):
            for code, valid in ((char + "A", char not in whitespace | controls),
                                ("A" + char, char not in whitespace | controls),
                                ("A" + char + "B", char not in controls)):
                with self.subTest(code=code):
                    entry["code"] = code
                    self.accepts(SEMANTIC, proposal, valid)

    def test_component_constraint_forms(self):
        for kind, key, good, bad in [
            ("integer", "minInclusive", "-9007199254740993", [1, "01", "-0", "1e2"]),
            ("decimal", "maxInclusive", "-0.005", [1.5, "+1", "1.00", "01.5", "1E3"]),
            ("date", "minInclusive", "2024-02-29",
             [20260101, "01/01/2026", "2026-13-01", "1900-02-29", "2024-04-31"]),
            ("datetime", "maxInclusive", "2026-01-01T00:00:00.000000001Z",
             [0, "2026-01-01", "2026-01-01T01:00:00+01:00"]),
            ("string", "maxLength", 12, ["12", 1.5, True]),
            ("concept", "vocabularyRef", "https://example.org/vocabolario/città",
             [42, "/vocabolario", "urn:due parole", "urn:bad%xx"]),
        ]:
            for value in [good, *bad]:
                with self.subTest(kind=kind, value=value):
                    component = {"name": "entry", "type": kind, "required": True,
                                 "cardinality": "ONE", "constraints": {key: value}}
                    self.accepts(SEMANTIC, {**self.proposal, "definition": {"components": [component]}},
                                 value == good)

    def field(self, mapping, valid=True):
        field = {"fieldName": "value", "extractionDecision": "INCLUDE", "dataAccessLabel": "OPEN"}
        self.accepts(CREATE, {**self.create, "fields": [{**field, **mapping}]}, valid)
        decision = {"column": "value", "decision": "MAP", "mapping": {
            "propertyIri": "urn:p", "reconciliation": {
                "policyRef": "urn:ouf:policy:linked-object-property-reconciliation:v1",
                "cardinality": "SINGLE", "normalizerRef": "EXACT_TYPED",
                "normalizerVersion": 1, "validity": "UNBOUNDED",
            }, **mapping,
        }}
        self.accepts(REVISION, {**self.revision, "newColumns": [decision]}, valid)

    def test_readers_and_parameters(self):
        self.accepts(CREATE, {**self.create, "fields": [{"fieldName": "value", "extractionDecision": "INCLUDE",
                                                        "dataAccessLabel": "OPEN", "targetPropertyIri": "urn:p"}]})
        for reader in self.contract["readers"]:
            with self.subTest(reader=reader["id"]):
                params = {k: {"crs": "EPSG:4326", "valueMapRef": PIN}[k] for k in reader.get("params", {})}
                mapping = {"transform": reader["id"]}
                if params:
                    self.field(mapping, False)
                    mapping["transformParams"] = params
                self.field(mapping)
        field = {"fieldName": "value", "extractionDecision": "INCLUDE", "dataAccessLabel": "OPEN",
                 "transform": "VALUE_MAP_V1", "valueMapRef": PIN}
        self.accepts(CREATE, {**self.create, "fields": [field]})
        self.field({"transform": "WKT_V1", "transformParams": {"crs": "4326"}}, False)
        self.field({"transform": "IDENTITY", "transformParams": {"other": 0}}, False)
        self.field({"transform": "UNKNOWN"}, False)

    def compositions(self, values, valid=True):
        fields = self.contract["mappingShape"]["example"]["fields"]
        self.accepts(CREATE, {**self.create, "fields": fields, "compositions": values}, valid)
        self.accepts(REVISION, {**self.revision, "compositions": values}, valid)
        same_file = {k: v for k, v in self.revision.items() if k != "assetRef"}
        self.accepts(REVISION, {**same_file, "compositions": values}, valid)

    def test_constructor_shapes(self):
        point, address = deepcopy(self.contract["mappingShape"]["example"]["compositions"])
        self.compositions([point, address])
        self.compositions([{**point, "constructorId": "UNKNOWN"}], False)
        self.compositions([{**point, "inputs": [point["inputs"][0]] * 2}], False)
        self.compositions([{**point, "inputs": [{**i, "reader": "UNKNOWN"} for i in point["inputs"]]}], False)
        item = {"role": "part", "fieldName": "via", "reader": "IDENTITY"}
        join = {**point, "constructorId": "TEXT_JOIN_V1", "inputs": [item] * 8, "params": {"separator": ", "}}
        self.compositions([join])
        self.compositions([{**join, "inputs": [item] * 9}], False)
        self.compositions([{**join, "params": {"separator": "x" * 9}}], False)
        split = {**join, "constructorId": "LIST_SPLIT_V1", "inputs": [{**item, "role": "value"}]}
        self.compositions([split], False)
        self.compositions([{**split, "params": {"separator": ",", "itemReader": "INTEGER_V1"}}])
        self.compositions([{**split, "params": {"separator": ",", "itemReader": "UNKNOWN"}}], False)

    def test_list_split_item_reader_params(self):
        split = {"kind": "CONSTRUCTOR", "constructorId": "LIST_SPLIT_V1",
                 "targetPropertyIri": "urn:list", "dataAccessLabel": "OPEN",
                 "inputs": [{"role": "value", "fieldName": "via", "reader": "IDENTITY"}]}
        for reader, options, valid in [
            ("WKT_V1", {"crs": "EPSG:3003"}, True),
            ("VALUE_MAP_V1", {"valueMapRef": PIN}, True),
            ("IDENTITY", {}, True),
            ("WKT_V1", None, False), ("VALUE_MAP_V1", {}, False),
            ("WKT_V1", {"crs": "4326"}, False),
            ("VALUE_MAP_V1", {"crs": "EPSG:4326"}, False),
            ("WKT_V1", {"crs": "EPSG:4326", "unexpected": 1}, False),
            ("IDENTITY", {"crs": "EPSG:4326"}, False), ("IDENTITY", [], False),
        ]:
            with self.subTest(reader=reader, options=options):
                params = {"itemReader": reader, "separator": ";"}
                if options is not None:
                    params["itemReaderParams"] = options
                self.compositions([{**split, "params": params}], valid)

    def test_many_item_builders(self):
        point, address = self.contract["mappingShape"]["example"]["compositions"]
        ctor = {k: point[k] for k in ("constructorId", "inputs", "params")}
        field = {"fieldName": "via", "reader": "IDENTITY"}
        nested = {k: address[k] for k in ("typeRef", "components")}
        for items, valid in [
            ([field, field], True), ([{"constructor": ctor}], True), ([{"structured": nested}], True),
            ({"fieldName": "via"}, False), ([], True), ([{}], False), ([{"items": [field]}], False),
            ([{**field, "constructor": ctor}], False), ([{"fieldName": "via"}], False),
            ([{"constructor": {"constructorId": "LIST_SPLIT_V1", "params": {
                "separator": ";", "itemReader": "IDENTITY"}, "inputs": [{"role": "value", **field}]}}], False),
        ]:
            with self.subTest(items=items):
                self.compositions([{**address, "components": [{"component": "parts", "items": items}]}], valid)
        for depth in range(2, 6):
            nested = {"typeRef": PIN, "components": [{"component": "parts", "items": [{"structured": nested}]}]}
            self.compositions([{**address, **nested}], depth <= 4)

    def test_dispatch_fixtures(self):
        for tool, request, invalid in dispatch_cases():
            with self.subTest(tool=tool):
                self.accepts(tool, request["Arguments"])
                self.accepts(tool, invalid["Arguments"], False)

    @unittest.skipUnless(os.environ.get("GATEWAY_PAIRWISE_ROOT"), "requires a task-46 Gateway checkout")
    def test_gateway_value_types_pairwise(self):
        schemas = Path(os.environ["GATEWAY_PAIRWISE_ROOT"]) / "schemas"
        for tool, request, invalid in dispatch_cases():
            with self.subTest(tool=tool):
                schema = json.loads((schemas / GATEWAY_SCHEMAS[tool]).read_bytes())
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema)
                self.assertTrue(validator.is_valid(request), "D-084 request rejected")
                self.assertFalse(validator.is_valid(invalid), "invalid D-084 request accepted")

    def test_nested_structured_shapes(self):
        point, address = deepcopy(self.contract["mappingShape"]["example"]["compositions"])
        ctor = {k: point[k] for k in ("constructorId", "inputs", "params")}
        nested = {"typeRef": PIN, "components": [{"component": "point", "constructor": ctor}]}
        for depth in range(1, 6):
            with self.subTest(depth=depth):
                self.compositions([{**address, **nested}], depth <= 4)
            nested = {"typeRef": PIN, "components": [{"component": "inner", "structured": nested}]}
        leaf = address["components"][0]
        for invalid in [{**leaf, "reader": "UNKNOWN"}, {**leaf, "constructor": ctor},
                        {"component": "x", "structured": {"typeRef": "urn:unpinned", "components": [leaf]}}]:
            self.compositions([{**address, "components": [invalid]}], False)
        wide = [{**leaf, "component": f"c{i}"} for i in range(33)]
        self.compositions([{**address, "components": wide[:32]}])
        self.compositions([{**address, "components": wide}], False)
        self.compositions([{**address, "components": []}], False)
        self.compositions([], True)
