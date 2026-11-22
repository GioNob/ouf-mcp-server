import hashlib
import json
import unittest
from copy import deepcopy

from jsonschema import Draft202012Validator

from scripts.generate_value_type_schemas import CONTRACT, DIGEST, MANIFEST, generate


PIN = "urn:example:address@00000000-0000-4000-8000-000000000001"
UUID = "00000000-0000-4000-8000-000000000002"
CREATE = "source.onboarding.create"
REVISION = "source.onboarding.revision.prepare"
SEMANTIC = "semantic.propose"


class ValueTypeSchemas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(CONTRACT.read_bytes())
        cls.tools = {c["toolName"]: c for c in json.loads(MANIFEST.read_bytes())["capabilities"]}
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
