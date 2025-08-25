# core/matching.py
from __future__ import annotations
import re
from utils.parse import strip_accents, std_text

def _norm_service_text(s: str | None) -> str:
    t = strip_accents(std_text(s))
    t = re.sub(r"[^A-Z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def service_code_from_tipo(tipo: str | None) -> str | None:
    """
    - RESERVADO MEDS     -> RESMD
    - ESTANDAR 2 MEDS    -> ST2MD
    - ESTANDAR 10 BASICO -> ST2MD
    - ESTANDAR 2 BASICO  -> ST2MD
    """
    t = _norm_service_text(tipo)
    if not t: return None
    if (re.search(r"\bRES(ERVADO)?\b", t) and "MED" in t) or "RESMD" in t:
        return "RESMD"
    has_est = re.search(r"\b(ESTANDAR|EST|STD)\b", t) is not None
    if has_est and re.search(r"\b2\b", t) and "MED" in t: return "ST2MD"
    if has_est and re.search(r"\b10\b", t) and ("BAS" in t or "BASIC" in t): return "ST2MD"
    if has_est and re.search(r"\b2\b", t) and ("BAS" in t or "BASIC" in t):  return "ST2MD"
    return None
