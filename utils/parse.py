from __future__ import annotations
import re, unicodedata
from datetime import date, datetime
from typing import List
import numpy as np
import pandas as pd
import re

# Funções de normalização e parsing de textos e numéricos, e códigos de serviços
def to_num(val):
    """Números pt-BR/US e datas acidentais → float (ex.: 13/08/2025 → 13.8)."""
    if isinstance(val, (int, float, np.number)) and not pd.isna(val):
        return float(val)
    if isinstance(val, (pd.Timestamp, datetime, date)):
        return float(f"{int(val.day)}.{int(val.month)}")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return np.nan
    s = str(val).strip()
    if s in ("", "-", "–", "—"): return np.nan
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})(?:/\d{2,4})?", s)
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        return float(f"{d}.{mo}")
    s = re.sub(r"(?i)r\$\s*", "", s).replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return np.nan

def strip_accents(s):
    if s is None: return ""
    return "".join(ch for ch in unicodedata.normalize("NFD", str(s)) if unicodedata.category(ch) != "Mn")

def std_text(s):
    if s is None: return ""
    return str(s).strip().upper()

def normalize_label(s: str) -> str:
    t = strip_accents(str(s or "")).upper()
    return re.sub(r"\s+", " ", t).strip()

def tokens(s: str | None) -> List[str]:
    t = strip_accents(str(s or "")).upper()
    return re.findall(r"[A-Z0-9]+", t)


def _norm_service_text(s: str | None) -> str:
    t = strip_accents(std_text(s))
    t = re.sub(r"[^A-Z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def service_code_from_tipo(tipo: str | None) -> str | None:
    """
    - VELOZ MEDS         -> RESMD
    - RESERVADO MEDS     -> RESMD
    - ESTANDAR 2 MEDS    -> ST2MD
    - ESTANDAR 10 BASICO -> ST2MD
    - ESTANDAR 2 BASICO  -> ST2MD
    """
    t = _norm_service_text(tipo)
    if not t: return None
    
    # MODIFICATION: Added 'VELOZ' to the list of keywords for the 'RESMD' code.
    # Now it checks for 'RESERVADO', 'RES', or 'VELOZ' along with 'MEDS'.
    if (re.search(r"\b(RES(ERVADO)?|VELOZ)\b", t) and "MED" in t) or "RESMD" in t:
        return "RESMD"
        
    has_est = re.search(r"\b(ESTANDAR|EST|STD)\b", t) is not None
    if has_est and re.search(r"\b2\b", t) and "MED" in t: return "ST2MD"
    if has_est and re.search(r"\b10\b", t) and ("BAS" in t or "BASIC" in t): return "ST2MD"
    if has_est and re.search(r"\b2\b", t) and ("BAS" in t or "BASIC" in t):  return "ST2MD"
    
    return None