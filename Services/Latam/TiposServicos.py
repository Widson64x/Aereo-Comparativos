# Services/service_types.py
from __future__ import annotations
import re
import unicodedata
from typing import Optional

# Conjunto canônico (exatamente como você pediu)
ALLOWED = [
    "MD/PE", "MD", "BA",
    "ST2MD", "ST2PE",
    "RESMD",
    "ST10B",
    "ST2BA", "ST3BA", "ST5BA",
    "MEDICAMENTOS",
]

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def normalize_tipo_servico(text: str | None) -> Optional[str]:
    """
    Detecta um dos serviços canônicos sem agrupar diferentes códigos.
    Regras:
      - "MD/PE" é distinto de "MD"
      - "BA" não deve capturar "ST2BA" / "ST3BA" / "ST5BA"
      - "RESMD" só mapeia explicitamente (não mapeia "Reservado" genérico)
    """
    if text is None:
        return None
    u = _strip_accents(str(text)).upper()
    # tolera separadores
    u = re.sub(r"[.\-_]", " ", u)

    # 1) Códigos mais longos primeiro (evita 'BA' capturar 'ST2BA')
    if re.search(r"\bST\s*10\s*B\b", u):
        return "ST10B"
    if re.search(r"\bST\s*5\s*BA\b", u):
        return "ST5BA"
    if re.search(r"\bST\s*3\s*BA\b", u):
        return "ST3BA"
    if re.search(r"\bST\s*2\s*BA\b", u):
        return "ST2BA"
    if re.search(r"\bST\s*2\s*MD\b", u):
        return "ST2MD"
    if re.search(r"\bST\s*2\s*PE\b", u):
        return "ST2PE"

    # 2) MD/PE (formas: "MD / PE", "MD-PE", "MDPE")
    if re.search(r"\bMD\s*/\s*PE\b", u) or re.search(r"\bMD\s*PE\b", u):
        return "MD/PE"

    # 3) RESMD (não associar "Reservado" genérico)
    if re.search(r"\bRESMD\b", u):
        return "RESMD"

    # 4) MEDICAMENTOS
    if re.search(r"\bMEDICAMENT", u):
        return "MEDICAMENTOS"

    # 5) MD, BA (simples, com cuidado)
    if re.search(r"\bMD\b", u):
        return "MD"
    # 'BA' simples, mas evite "ST n BA" (já capturado acima)
    if re.search(r"(^|[\s/])BA($|[\s/])", u):
        return "BA"

    return None
