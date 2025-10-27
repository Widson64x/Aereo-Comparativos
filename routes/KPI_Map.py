# C:\Programs\Aéreo-Comparativos\Routes\KPI_Map.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from flask import Blueprint, current_app, jsonify, render_template, request, url_for

from Config import Appconfig

bp = Blueprint("kpi_map", __name__, template_folder="../Templates")

SAO_IATAS = ["CGH", "GRU", "VCP"]
EPS = 1e-6  # para diferença de valor

@dataclass
class IATANode:
    iata: str
    name: str
    region: str
    country: str
    lat: float
    lon: float
    in_count: int = 0
    out_count: int = 0
    degree: int = 0
    sum_frete: float = 0.0
    sum_tarifa: float = 0.0


# ----------------------- paths -----------------------
def _paths() -> Tuple[Appconfig, Path]:
    cfg: Appconfig = current_app.config["APP_CFG"]
    return cfg, cfg.paths.CACHE_DIR

def _project_data_dir() -> Path:
    base = Path(current_app.root_path)
    d1 = base / "Data"
    d2 = base / "data"
    return d1 if d1.exists() else d2


# ----------------------- base loads ------------------
def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _load_batch_feather(batch_id: str) -> pd.DataFrame:
    _, cache_dir = _paths()
    path = cache_dir / f"batch_{batch_id}.feather"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_feather(path)
    except Exception:
        return pd.DataFrame()

    # normalizações e colunas necessárias para BI
    want = [
        "Origem", "Destino", "Valor_Frete", "Valor_Tarifa", "Status", "Documento",
        "Tipo_Serviço", "Data",
        "Valor_Frete_Tabela", "Valor_Tarifa_Tabela",
        "Diferenca_Frete", "Diferenca_Tarifa", "Dif_%",
        "Peso Taxado"
    ]
    # renome de legado
    rename_map = {}
    for c in df.columns:
        if str(c).strip().lower() == "tipo_servico":
            rename_map[c] = "Tipo_Serviço"
        if str(c).strip().lower() == "peso_taxado":
            rename_map[c] = "Peso Taxado"
    df = df.rename(columns=rename_map)

    for col in want:
        if col not in df.columns:
            df[col] = np.nan

    # limpeza
    df["Origem"]  = df["Origem"].astype(str).str.strip().str.upper()
    df["Destino"] = df["Destino"].astype(str).str.strip().str.upper()
    for c in ["Valor_Frete","Valor_Tarifa","Valor_Frete_Tabela","Valor_Tarifa_Tabela",
              "Diferenca_Frete","Diferenca_Tarifa","Dif_%","Peso Taxado"]:
        df[c] = _safe_num(df[c])

    df["Status"] = df["Status"].astype(str).str.strip()
    # flags
    df["__COM_DIF__"] = (df["Diferenca_Frete"].abs() > EPS) | (df["Diferenca_Tarifa"].abs() > EPS)
    df["__DEV__"] = df["Status"].eq("DEVOLUCAO")
    df["__SEM_TARIFA__"] = df["Status"].eq("TARIFA NAO LOCALIZADA")
    df["__FRETE_MIN__"] = df["Status"].eq("FRETE MINIMO")
    df["__PESO_EXC__"] = df["Status"].eq("PESO EXCEDENTE")
    return df

def _load_iata_master() -> pd.DataFrame:
    csv_local = _project_data_dir() / "iata-icao.csv"
    if not csv_local.exists():
        raise FileNotFoundError(f"Arquivo IATA não encontrado: {csv_local}")

    df = pd.read_csv(csv_local)
    keep = ["iata", "airport", "region_name", "country_code", "latitude", "longitude"]
    for k in keep:
        if k not in df.columns:
            df[k] = np.nan
    df = df[keep].copy()
    df["iata"] = df["iata"].astype(str).str.upper().str.strip()
    df["latitude"]  = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["iata", "latitude", "longitude"])
    return df


# ----------------------- aggregates ------------------
def _expand_aliases(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        origs = SAO_IATAS if row["Origem"] == "SAO" else [row["Origem"]]
        dests = SAO_IATAS if row["Destino"] == "SAO" else [row["Destino"]]
        for o in origs:
            for d in dests:
                newr = row.copy()
                newr["Origem"] = o
                newr["Destino"] = d
                rows.append(newr)
    return pd.DataFrame(rows, columns=df.columns)

def _aggregate_routes(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = df[df["Origem"].str.match(r"^[A-Z]{3}$", na=False)]
    df = df[df["Destino"].str.match(r"^[A-Z]{3}$", na=False)]
    df = _expand_aliases(df)

    base_aggs = {
        "Valor_Frete": "sum",
        "Valor_Tarifa": "sum",
        "Documento": "count",
        "__COM_DIF__": "sum",
        "__DEV__": "sum",
        "__SEM_TARIFA__": "sum",
        "__FRETE_MIN__": "sum",
        "__PESO_EXC__": "sum",
        "Peso Taxado": "sum",
    }
    g = (
        df.groupby(["Origem","Destino"], as_index=False)
          .agg(base_aggs)
          .rename(columns={
              "Documento": "Qtde_Docs",
              "Valor_Frete":"Soma_Frete",
              "Valor_Tarifa":"Soma_Tarifa",
              "__COM_DIF__":"COM_DIF",
              "__DEV__":"DEVOLUCAO",
              "__SEM_TARIFA__":"TARIFA NAO LOCALIZADA",
              "__FRETE_MIN__":"FRETE MINIMO",
              "__PESO_EXC__":"PESO EXCEDENTE",
              "Peso Taxado":"Soma_Peso"
          })
    )

    gm = (
        df.groupby(["Origem","Destino"], as_index=False)[["Valor_Frete","Valor_Tarifa","Peso Taxado"]]
          .mean()
          .rename(columns={"Valor_Frete":"Media_Frete","Valor_Tarifa":"Media_Tarifa","Peso Taxado":"Media_Peso"})
    )
    g = g.merge(gm, on=["Origem","Destino"], how="left")

    nodes_df = pd.DataFrame({"IATA": pd.unique(pd.concat([df["Origem"], df["Destino"]], ignore_index=True))})
    out_counts = g.groupby("Origem")["Qtde_Docs"].sum().rename("out_count")
    in_counts  = g.groupby("Destino")["Qtde_Docs"].sum().rename("in_count")
    out_val    = g.groupby("Origem")["Soma_Frete"].sum().rename("sum_out_frete")
    in_val     = g.groupby("Destino")["Soma_Frete"].sum().rename("sum_in_frete")
    nodes_df = (nodes_df
                .merge(out_counts, left_on="IATA", right_index=True, how="left")
                .merge(in_counts,  left_on="IATA", right_index=True, how="left")
                .merge(out_val,    left_on="IATA", right_index=True, how="left")
                .merge(in_val,     left_on="IATA", right_index=True, how="left"))
    nodes_df[["out_count","in_count","sum_out_frete","sum_in_frete"]] = nodes_df[
        ["out_count","in_count","sum_out_frete","sum_in_frete"]
    ].fillna(0)

    deg_out = g.groupby("Origem")["Destino"].nunique().rename("deg_out")
    deg_in  = g.groupby("Destino")["Origem"].nunique().rename("deg_in")
    nodes_df = (nodes_df.merge(deg_out, left_on="IATA", right_index=True, how="left")
                         .merge(deg_in,  left_on="IATA", right_index=True, how="left"))
    nodes_df[["deg_out","deg_in"]] = nodes_df[["deg_out","deg_in"]].fillna(0)
    nodes_df["degree"] = nodes_df["deg_out"] + nodes_df["deg_in"]
    return g, nodes_df

def _attach_coords(g: pd.DataFrame, nodes_df: pd.DataFrame, iata_master: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    info = iata_master.set_index("iata")[["airport","region_name","country_code","latitude","longitude"]]

    nodes = (nodes_df.merge(info, left_on="IATA", right_index=True, how="left")
                    .rename(columns={"airport":"name","region_name":"region","country_code":"country",
                                     "latitude":"lat","longitude":"lon"}))
    nodes = nodes.dropna(subset=["lat","lon"])

    src = info.rename(columns={"latitude":"o_lat","longitude":"o_lon"}).reset_index().rename(columns={"iata":"Origem"})
    dst = info.rename(columns={"latitude":"d_lat","longitude":"d_lon"}).reset_index().rename(columns={"iata":"Destino"})
    ge  = g.merge(src, on="Origem", how="left").merge(dst, on="Destino", how="left")
    ge  = ge.dropna(subset=["o_lat","o_lon","d_lat","d_lon"])
    return ge, nodes


# ------------------------- views ---------------------
# Página isolada opcional
@bp.get("/map/<batch_id>")
def view_map(batch_id: str):
    back_url = url_for("fatura.compare_batch_page", batch_id=batch_id)
    return render_template("Tools/KPI_Map.html", batch_id=batch_id, back_url=back_url)

# JSON para o componente
@bp.get("/map/data/<batch_id>")
def map_data(batch_id: str):
    df = _load_batch_feather(batch_id)
    if df.empty:
        return jsonify({"meta":{"batch_id":batch_id,"rows":0},"nodes":[],"links":[]})

    g, nodes_df = _aggregate_routes(df)
    try:
        iata_master = _load_iata_master()
    except FileNotFoundError as e:
        return jsonify({"meta":{"batch_id":batch_id,"rows":int(len(df))},"error":str(e),"nodes":[],"links":[]}), 500

    ge, nodes = _attach_coords(g, nodes_df, iata_master)

    known_base = {"Origem","Destino","Soma_Frete","Soma_Tarifa","Qtde_Docs","Media_Frete","Media_Tarifa",
                  "o_lat","o_lon","d_lat","d_lon","Soma_Peso","Media_Peso"}
    status_cols = [c for c in ge.columns if c not in known_base and pd.api.types.is_numeric_dtype(ge[c])]

    links: List[Dict] = []
    for _, r in ge.iterrows():
        sdict = {sc: float(r[sc]) for sc in status_cols if pd.notna(r[sc])}
        links.append({
            "o": r["Origem"], "d": r["Destino"],
            "count": int(r["Qtde_Docs"]),
            "sum_frete": float(r["Soma_Frete"]),
            "sum_tarifa": float(r["Soma_Tarifa"]),
            "sum_peso": float(r.get("Soma_Peso", 0) or 0),
            "avg_frete": float(r["Media_Frete"]) if pd.notna(r["Media_Frete"]) else 0.0,
            "avg_tarifa": float(r["Media_Tarifa"]) if pd.notna(r["Media_Tarifa"]) else 0.0,
            "avg_peso": float(r.get("Media_Peso", 0) or 0),
            "o_lat": float(r["o_lat"]), "o_lon": float(r["o_lon"]),
            "d_lat": float(r["d_lat"]), "d_lon": float(r["d_lon"]),
            "status": sdict,
        })

    nodes_json: List[Dict] = []
    for _, r in nodes.iterrows():
        nodes_json.append({
            "iata": r["IATA"], "name": r.get("name","") or "",
            "region": r.get("region","") or "", "country": r.get("country","") or "",
            "lat": float(r["lat"]), "lon": float(r["lon"]),
            "in_count": int(r.get("in_count",0) or 0), "out_count": int(r.get("out_count",0) or 0),
            "degree": int(r.get("degree",0) or 0),
            "sum_frete": float((r.get("sum_out_frete",0) or 0) + (r.get("sum_in_frete",0) or 0)),
        })

    return jsonify({"meta":{"batch_id":batch_id,"rows":int(len(df))},"nodes":nodes_json,"links":links})


# ---------- Drill-down: rota (lista de itens) ----------
@bp.get("/map/route/items/<batch_id>")
def route_items(batch_id: str):
    """Retorna HTML de tabela filtrada por rota, com filtros opcionais."""
    o = (request.args.get("o") or "").upper().strip()
    d = (request.args.get("d") or "").upper().strip()
    limit = int(request.args.get("limit") or 500)
    only_diff = (request.args.get("diff") or "0") in {"1","true","True"}
    only_dev = (request.args.get("dev") or "0") in {"1","true","True"}
    only_sem = (request.args.get("sem") or "0") in {"1","true","True"}
    only_min = (request.args.get("min") or "0") in {"1","true","True"}
    only_pesoexc = (request.args.get("pex") or "0") in {"1","true","True"}

    df = _load_batch_feather(batch_id)
    if df.empty:
        return "<div class='text-muted'>Sem dados.</div>"

    df = df[(df["Origem"] == o) & (df["Destino"] == d)]
    if only_diff:    df = df[df["__COM_DIF__"]]
    if only_dev:     df = df[df["__DEV__"]]
    if only_sem:     df = df[df["__SEM_TARIFA__"]]
    if only_min:     df = df[df["__FRETE_MIN__"]]
    if only_pesoexc: df = df[df["__PESO_EXC__"]]

    if df.empty:
        return "<div class='text-muted'>Sem itens para os filtros.</div>"

    cols = [
        "Documento","Data","Tipo_Serviço","Origem","Destino","Peso Taxado",
        "Valor_Frete","Valor_Tarifa","Valor_Frete_Tabela","Valor_Tarifa_Tabela",
        "Diferenca_Frete","Diferenca_Tarifa","Dif_%","Status"
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].copy()
    # Data em dd/mm/yyyy
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%d/%m/%Y")

    df = df.head(limit)
    html = df.to_html(classes="table table-sm table-striped table-hover", index=False, na_rep="—", justify="left")
    return html


# ---------- Drill-down: nó (resumo + itens) ----------
@bp.get("/map/node/summary/<batch_id>")
def node_summary(batch_id: str):
    iata = (request.args.get("iata") or "").upper().strip()
    if not iata:
        return jsonify({"error":"iata faltando"}), 400

    df = _load_batch_feather(batch_id)
    if df.empty:
        return jsonify({"iata": iata, "inbound":{}, "outbound":{}, "top_in":[], "top_out":[]})

    inbound  = df[df["Destino"] == iata]
    outbound = df[df["Origem"]  == iata]

    def _sum_obj(dd: pd.DataFrame) -> Dict:
        return {
            "docs": int(len(dd)),
            "sum_frete": float(dd["Valor_Frete"].fillna(0).sum()),
            "sum_tarifa": float(dd["Valor_Tarifa"].fillna(0).sum()),
            "sum_peso": float(dd["Peso Taxado"].fillna(0).sum()),
            "dev": int(dd["__DEV__"].sum()),
            "sem_tarifa": int(dd["__SEM_TARIFA__"].sum()),
            "frete_min": int(dd["__FRETE_MIN__"].sum()),
            "peso_exc": int(dd["__PESO_EXC__"].sum()),
            "com_dif": int(dd["__COM_DIF__"].sum()),
        }

    def _top(dd: pd.DataFrame, by_cols: List[str]) -> List[Dict]:
        if dd.empty: return []
        gg = (dd.groupby(by_cols, as_index=False)
                .agg({"Documento":"count","Valor_Frete":"sum","__COM_DIF__":"sum"})
                .rename(columns={"Documento":"docs","Valor_Frete":"frete","__COM_DIF__":"com_dif"})
                .sort_values(by=["docs","frete"], ascending=False)
                .head(10))
        return gg.to_dict(orient="records")

    top_in  = _top(inbound,  ["Origem","Destino"])
    top_out = _top(outbound, ["Origem","Destino"])

    return jsonify({
        "iata": iata,
        "inbound":  _sum_obj(inbound),
        "outbound": _sum_obj(outbound),
        "top_in":  top_in,
        "top_out": top_out
    })

@bp.get("/map/node/items/<batch_id>")
def node_items(batch_id: str):
    iata = (request.args.get("iata") or "").upper().strip()
    direction = (request.args.get("dir") or "in").lower()  # in | out
    limit = int(request.args.get("limit") or 500)

    df = _load_batch_feather(batch_id)
    if df.empty:
        return "<div class='text-muted'>Sem dados.</div>"

    if direction == "out":
        df = df[df["Origem"] == iata]
    else:
        df = df[df["Destino"] == iata]

    cols = [
        "Documento","Data","Tipo_Serviço","Origem","Destino","Peso Taxado",
        "Valor_Frete","Valor_Tarifa","Valor_Frete_Tabela","Valor_Tarifa_Tabela",
        "Diferenca_Frete","Diferenca_Tarifa","Dif_%","Status"
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].copy()
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
    df = df.head(limit)
    html = df.to_html(classes="table table-sm table-striped table-hover", index=False, na_rep="—", justify="left")
    return html
