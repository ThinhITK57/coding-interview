import requests
import os
from urllib.parse import quote

import urllib3

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

class OMClient:
    def __init__(self, host: str=None, token: str=None):
        if host is None:
            host = os.getenv("OM_HOST")
        if token is None:
            token = os.getenv("OM_TOKEN")
        
        print(f"host={host}")
        
        self.host = host.rstrip("/")
        self.token = token

    # -------------------------
    # CORE REQUEST
    # -------------------------
    def _headers(self):

        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get(self, path, params=None):

        url = f"{self.host}{path}"

        res = requests.get(url, headers=self._headers(), params=params, verify=False)

        res.raise_for_status()

        return res.json()

    def post(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.post(url, headers=self._headers(), json=json, verify=False)

        res.raise_for_status()

        return res.json() if res.text else None

    def put(self, path, json=None):
    
        url = f"{self.host}{path}"

        res = requests.put(url, headers=self._headers(), json=json, verify=False)

        res.raise_for_status()

        return res.json() if res.text else None
    
    def patch(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.patch(url, headers={
                **self._headers(),
                "Content-Type": "application/json-patch+json",
            }, json=json, verify=False)

        res.raise_for_status()

        return res.json() if res.text else None

    def delete(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.delete(url, headers=self._headers(), json=json, verify=False)

        res.raise_for_status()

        return res.json() if res.text else None
    
    
class TablesAPI:
    def __init__(self, client: OMClient):
        self.client = client

    def get_by_fqn(self, fqn: str):
        return self.client.get(
            f"/api/v1/tables/name/{fqn}"
        )

    def get_id(self, fqn: str):
        return self.get_by_fqn(fqn)["id"]
    
    
    def create(self, payload: dict):
        return self.client.post(
            "/api/v1/tables",
            json=payload
        )
    
    def ensure_table(self, fqn):
        try:
            return self.create(self.build_table_payload(fqn))
        except Exception as e:
            # race condition safe
            print(e)
            return self.get_by_fqn(fqn)
        
        
    def build_table_payload(self, fqn: str):
        service, db_schema, table = fqn.split(".", 2)
        return {
            "name": table,
            "fullyQualifiedName": fqn,
            "databaseSchema": db_schema,
            "service": service,
            "columns": [],
            "tableType": "Regular"
        }
        
    def safe_get_table_id(self, fqn: str):
        table = self.get_by_fqn(fqn)
        if table:
            return table["id"]
        # auto-create
        table = self.ensure_table(fqn)
        return table["id"]
    
    
    def patch_table_description(self, table_id, description):
        patch = [
            {
                "op": "replace",
                "path": "/description",
                "value": description,
            }
        ]

        return self.client.patch(
            f"/api/v1/tables/{table_id}",
            json=patch,
        )

    def update(self, table_id,  entity: dict):
        return self.client.patch(
            f"/api/v1/tables/{table_id}",
            json=entity
        )

    def update_by_name(self, fqn,  entity: dict):
        return self.client.patch(
            f"/api/v1/tables/name/{fqn}",
            json=entity
        )
    
    
class LineageAPI:
    def __init__(self, client: OMClient):
        self.client: OMClient = client

    def create_edge(
        self,
        source_id: str,
        target_id: str,
        pipeline_id: str = None,
        description: str = ""
    ):

        payload = {
            "edge": {
                "fromEntity": {
                    "id": source_id,
                    "type": "table"
                },
                "toEntity": {
                    "id": target_id,
                    "type": "table"
                },
                "description": description
            }
        }

        if pipeline_id:
            payload["edge"]["lineageDetails"]= {
                "pipeline": {
                    "id": pipeline_id,
                    "type": "pipeline"
                }
            }

        return self.client.put(
            "/api/v1/lineage",
            json=payload
        )

    def delete_edge(self, source_id, target_id):

        payload = {
            "fromEntity": {
                "id": source_id,
                "type": "table"
            },
            "toEntity": {
                "id": target_id,
                "type": "table"
            }
        }

        return self.client.delete(
            "/api/v1/lineage",
            json=payload
        )

    def get_downstream(self, table_id):

        return self.client.get(
            f"/api/v1/lineage/{table_id}?direction=downstream"
        )
        
class PipelinesAPI:
    def __init__(self, client: OMClient):
        self.client = client

    def create(self, payload: dict):
        return self.client.put(
            "/api/v1/pipelines",
            json=payload
        )

    def get_by_name(self, name: str):
        return self.client.get(
            f"/api/v1/pipelines/name/{name}"
        )

    def get_id(self, name: str):
        return self.get_by_name(name)["id"]
    
    
    
class GlossaryAPI:
    def __init__(self, client: OMClient):
        self.client = client

    def get_by_name(self, name: str):

        return self.client.get(
            f"/api/v1/glossaries/name/{name}"
        )

    def get_id(self, name: str):

        return self.get_by_name(name)["id"]

    def create_term(self, name, description, glossary_name):

        payload = {
            "name": name,
            "displayName": name,
            "description": description,
            "glossary": glossary_name
        }

        return self.client.post(
            "/api/v1/glossaryTerms",
            json=payload
        )
    
    def get_term_by_fqn(
        self,
        fqn: str,
    ):
        try:
            return self.client.get(
                f"/api/v1/glossaryTerms/name/{quote(fqn)}"
            )
        except Exception:
            return None
        
    def get_term(
        self,
        glossary_name: str,
        term_name: str
    ):
        fqn = f"{glossary_name}.{term_name}"

        try:
            return self.client.get(
                f"/api/v1/glossaryTerms/name/{quote(fqn)}"
            )
        except Exception:
            return None

    def create_child_term(
        self,
        glossary_name,
        parent_name,
        term_name,
        description,
        synonyms=None,
    ):

        payload = {
            "name": term_name,
            "displayName": term_name,
            "description": description,
            "glossary": glossary_name,
            "reviewers": [],
            "relatedTerms": [],
            "synonyms": synonyms or [],
            "mutuallyExclusive": False,
            "tags": [],
            "style": {},
            "parent": glossary_name+"."+parent_name
        }

        return self.client.post(
            "/api/v1/glossaryTerms",
            json=payload
        )
        
    def create_term(
        self,
        glossary_name: str,
        term_name: str,
        description: str,
        owner_id: str | None = None,
    ):

        payload = {
            "name": term_name,
            "displayName": term_name,
            "description": description,
            "reviewers": [],
            "relatedTerms": [],
            "synonyms": [],
            "mutuallyExclusive": False,
            "tags": [],
            "style": {},
            "glossary": glossary_name,
        }

        if owner_id:
            payload["owners"] = [
                {
                    "id": owner_id,
                    "type": "user"
                }
            ]

        return self.client.post(
            "/api/v1/glossaryTerms",
            json=payload
        )
    
    def patch_term_description(
        self,
        term_id: str,
        description: str
    ):
        patches = [
            {
                "op": "replace",
                "path": "/description",
                "value": description
            }
        ]

        return self.client.patch(
            f"/api/v1/glossaryTerms/{term_id}",
            json=patches,
        )
        
    def patch_term(
        self,
        term,
        description,
        synonyms
    ):

        patches = []

        current_desc = (
            term.get("description") or ""
        ).strip()

        if current_desc != description:

            patches.append({
                "op": "replace",
                "path": "/description",
                "value": description
            })

        current_synonyms = sorted(
            term.get("synonyms") or []
        )

        new_synonyms = sorted(
            synonyms or []
        )

        if current_synonyms != new_synonyms:

            patches.append({
                "op": "replace",
                "path": "/synonyms",
                "value": new_synonyms
            })

        if not patches:
            return term

        return self.client.patch(
            f"/api/v1/glossaryTerms/{term['id']}",
            json=patches,
        )
        
    def upsert_child_term(
        self,
        glossary_name,
        parent_name,
        term_name,
        description,
        synonyms=None,
    ):
        parent_fqn = (
            f"{glossary_name}.{parent_name}"
        )

        parent = self.get_term_by_fqn(
            parent_fqn
        )

        if not parent:
            raise Exception(
                f"Parent term not found: "
                f"{parent_fqn}"
            )

        child_fqn = (
            f"{glossary_name}."
            f"{parent_name}."
            f"{term_name}"
        )

        term = self.get_term_by_fqn(
            child_fqn
        )

        if not term:

            print(
                f"Creating term: "
                f"{child_fqn}"
            )

            return self.create_child_term(
                glossary_name=glossary_name,
                parent_name=parent_name,
                term_name=term_name,
                description=description,
                synonyms=synonyms,
            )

        print(
            f"Updating term: "
            f"{child_fqn}"
        )

        return self.patch_term(
            term,
            description,
            synonyms,
        )


class TestcaseAPI:
    
    def __init__(self, client: OMClient):
        self.client = client

    def create_test_case(
        self,
        table_fqn,
        column,
        test_definition,
        name,
    ):

        payload = {
            "name": name,
            "entityLink": (
                f"<#E::table::{table_fqn}::columns::{column}>"
                if column
                else f"<#E::table::{table_fqn}>"
            ),
            "testDefinition": test_definition,
        }
        r = self.client.post(
            f"/api/v1/dataQuality/testCases",
            json=payload,
        )
        return r

class OpenMetadataSDK:
    def __init__(self):

        self.client = OMClient()

        self.tables = TablesAPI(self.client)
        self.lineage = LineageAPI(self.client)
        self.pipelines = PipelinesAPI(self.client)
        self.glossary = GlossaryAPI(self.client)
        self.testcases = TestcaseAPI(self.client)
