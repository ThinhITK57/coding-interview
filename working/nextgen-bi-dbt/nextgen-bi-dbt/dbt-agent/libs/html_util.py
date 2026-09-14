from html import escape
import unicodedata

def to_html_description(text: str) -> str:
    if not text:
        return ""

    lines = [
        line.strip()
        for line in str(text).splitlines()
        if line.strip()
    ]

    return "".join(
        f"<p>{escape(line)}</p>"
        for line in lines
    )
    
import re

def normalize_term_name(name: str) -> str:
    if not name:
        return ""

    name = str(name)

    # bỏ newline
    name = name.replace("\r", " ")
    name = name.replace("\n", " ")
    name = name.replace("(", "-")
    name = name.replace(")", "-")

    # collapse whitespace
    name = re.sub(r"\s+", " ", name)

    return name.strip()

def sanitize_term_name(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\n", " ")

    # bỏ dấu tiếng Việt
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))

    text = text.lower()

    # chỉ giữ a-z0-9
    text = re.sub(r"[^a-z0-9]+", "_", text)

    text = re.sub(r"_+", "_", text)

    return text.strip("_")