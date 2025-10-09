# config.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

@dataclass(frozen=True)
class Paths:
    BASE_DIR: Path
    UPLOAD_DIR: Path
    OUTPUT_DIR: Path
    CACHE_DIR: Path

@dataclass(frozen=True)
class Columns:
    TARGET_COLS: List[str] = field(default_factory=lambda: [
        "Tipo_Serviço", "Origem", "Data", "Destino", "Vlr Frete", "Outras Taxas",
        "Peso Taxado", "Vlr Total", "Numero Fiscal", "Documento", "Vlr Advalorem",
        "Tipo de Cte",
    ])

@dataclass(frozen=True)
class Servicesconfig:
    # Ordem importa (mais específico primeiro)
    VALID_CODES_ST2: List[str] = field(default_factory=lambda: ["ST2MD", "MD/PE", "MEDICAMENTOS", "MD"])
    RESERVADO_CODE: str = "RESMD"
    ESTANDAR_CODE: str = "ST2MD"  # mapeamos ESTANDAR → ST2MD p/ lookup

@dataclass(frozen=True)
class IOconfig:
    ALLOWED_PDF_EXTS: List[str] = field(default_factory=lambda: [".pdf"])
    ALLOWED_EXCEL_EXTS: List[str] = field(default_factory=lambda: [".xlsx"])

@dataclass(frozen=True)
class Tuning:
    TOLERANCIA_PCT_DEFAULT: float = 1.0   # ±1.00%
    EPSILON: float = 1e-9                 # zera ruído float tipo 1.776e-15

@dataclass(frozen=True)
class Appconfig:
    paths: Paths
    cols: Columns = Columns()
    Services: Servicesconfig = Servicesconfig()
    io: IOconfig = IOconfig()
    tuning: Tuning = Tuning()
