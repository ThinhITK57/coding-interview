import requests
import os

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

        res = requests.get(url, headers=self._headers(), params=params)

        res.raise_for_status()

        return res.json()

    def post(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.post(url, headers=self._headers(), json=json)

        res.raise_for_status()

        return res.json() if res.text else None

    def put(self, path, json=None):
    
        url = f"{self.host}{path}"

        res = requests.put(url, headers=self._headers(), json=json)

        res.raise_for_status()

        return res.json() if res.text else None
    
    def patch(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.patch(url, headers={
                **self._headers(),
                "Content-Type": "application/json-patch+json",
            }, json=json)

        res.raise_for_status()

        return res.json() if res.text else None

    def delete(self, path, json=None):

        url = f"{self.host}{path}"

        res = requests.delete(url, headers=self._headers(), json=json)

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
            headers={
                **self.headers(),
                "Content-Type": "application/json-patch+json",
            },
        )

    def update(self, entity: dict):
        return self.client.put(
            "/api/v1/tables",
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
        


class OpenMetadataSDK:
    def __init__(self):

        self.client = OMClient()

        self.tables = TablesAPI(self.client)
        self.lineage = LineageAPI(self.client)
        self.pipelines = PipelinesAPI(self.client)
        self.glossary = GlossaryAPI(self.client)
