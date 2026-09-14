from .om_client import OpenMetadataSDK
from dataclasses import dataclass, field
import pandas as pd
from .html_util import to_html_description, normalize_term_name

@dataclass
class GlossaryTerm:
    name: str
    description: str = ""
    synonyms: list[str] = field(default_factory=list)
    children: list["GlossaryTerm"] = field(default_factory=list)
    
    
def upsert_term( om: OpenMetadataSDK, glossary_name: str, term_name: str,  description: str,  owner_id: str | None = None):
    term = om.glossary.get_term(
        glossary_name,
        term_name
    )

    if not term:

        print(
            f"Creating term: "
            f"{glossary_name}.{term_name}"
        )

        return om.glossary.create_term(
            glossary_name=glossary_name,
            term_name=term_name,
            description=description,
            owner_id=owner_id,
        )

    patches = []

    current_desc = (
        term.get("description") or ""
    ).strip()

    new_desc = (
        description or ""
    ).strip()

    if current_desc != new_desc:

        patches.append({
            "op": "replace",
            "path": "/description",
            "value": description
        })

    if not patches:

        print(
            f"No changes: "
            f"{glossary_name}.{term_name}"
        )
        return term

    print(
        f"Updating term: "
        f"{glossary_name}.{term_name}"
    )

    return om.client.patch(
        f"/api/v1/glossaryTerms/{term['id']}",
        json=patches,
    )
    

def upsert_term_by_fqn( om, fqn: str, description: str, owner_id=None):
    glossary_name, term_name = fqn.split(".", 1)
    return upsert_term(
        om,
        glossary_name,
        term_name,
        description,
        owner_id
    )
    


def ensure_parent_term( om,    glossary_name,    object_type):
    return upsert_term(
        om,
        glossary_name=glossary_name,
        term_name=object_type,
        description=f"<p>{object_type}</p>"
    )
    

def load_glossary_excel_crm(path):

    xls = pd.ExcelFile(path)

    result = {}

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(
            xls,
            sheet_name=sheet_name
        )
        glossary = {}
        for _, row in df.iterrows():
            level1 = str(
                row.get("Object Type", "")
            ).strip()

            level2 = str(
                row.get("Term", "")
            ).strip()

            if not level1 or not level2:
                continue

            synonyms = []

            if pd.notna(row.get("Synonyms / Alias")):

                synonyms = [
                    x.strip()
                    for x in str(
                        row["Synonyms / Alias"]
                    ).split(",")
                    if x.strip()
                ]

            definition = (
                str(row.get("Definition", ""))
                if pd.notna(row.get("Definition"))
                else ""
            )

            description = definition

            glossary.setdefault(
                level1,
                []
            ).append(
                {
                    "name": level2,
                    "description": description,
                    "synonyms": synonyms,
                }
            )
        result[sheet_name] = glossary
    return result


def sync_excel_glossary( excel_path: str):
    om: OpenMetadataSDK = OpenMetadataSDK()
    
    data = load_glossary_excel_crm(excel_path)

    for glossary_name, groups in data.items():
        glossary_name = normalize_term_name(glossary_name)
        
        print(
            f"Glossary: {glossary_name}"
        )

        for object_type, terms in groups.items():
            object_type = normalize_term_name(object_type)
            parent = ensure_parent_term(
                om,
                glossary_name,
                object_type
            )
            print(parent)

            for term in terms:
                try:
                    om.glossary.upsert_child_term(
                        glossary_name=glossary_name,
                        parent_name=object_type,
                        term_name=normalize_term_name(term["name"]),
                        description=to_html_description(term["description"]),
                        synonyms=term["synonyms"]
                    )
                except Exception as e:
                    print("ERROR", object_type, terms)
                    print(e)