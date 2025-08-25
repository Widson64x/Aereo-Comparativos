# Aéreo — Comparativos, Extração e Dashboards

> **Repo**: `Widson64x/Aereo-Comparativos`  
> **Objetivo**: extrair faturas **(PDF → Excel)**, enriquecer com métricas (ex.: **Frete Peso**), **comparar com acordos** (planilhas), e exibir **dashboards/KPIs** via web (Flask).  
> **Stack**: Python 3.10+ • Flask • Pandas • pdfplumber (+ Camelot opcional) • OpenPyXL • Matplotlib • Jinja2 • Bootstrap

---

## ✨ O que o sistema faz

- **PDF → Tabelas/Excel**: extrai as tabelas da fatura e já adiciona a coluna **`Frete Peso`** = `Vlr Frete / Peso Taxado`.
- **Comparativos com Acordos (XLSX)**:
  - Prioriza **`Reservado LUFT - Bases`**; se não existir, usa as demais abas com colunas **Origem/Destino/Valor/Tratamento** (variações normalizadas).
  - **`Tipo_Serviço`** gerado a partir do rótulo de **Tratamento** (ex.: `RESERVADO MEDS → RESMD`, `ESTANDAR 10 BÁSICO`, `VELOZ MEDS` etc.).
  - **Renomeações** na exportação: `Documento → nOca`, `Frete_Acordo → Frete_Tabela`; remove `Dif_abs` e `Observacao`.
- **Regras de Negócio**:
  - **Frete Mínimo**: se `Vlr Frete` ≤ 60 → classifica como **Frete Mínimo** (sem cálculo de diferença).
  - **Tolerância**: marca **Dentro/Fora da tolerância** conforme parâmetros do ambiente.
- **Dashboards/KPIs** com filtro por ano (matplotlib) e indicadores de status, faturamento e comparativos.
- **Interface Web** com upload de arquivos, pré-visualização e **download** do Excel processado.
- **Logs** (pasta configurável).

---

## 🧩 Estrutura do repositório

> Organização conforme o diretório publicado (pastas principais + arquivos de raiz).

```
core/               # Domínio e utilitários centrais do projeto
formats/            # Normalizações e formatações de dados/colunas
repositories/       # Acesso a planilhas e funções de leitura (e.g., acordos)
routes/             # Blueprints/rotas Flask (upload, export, dashboards)
services/           # Regras de negócio (comparativos, tolerância, etc.)
static/             # CSS/JS/Imagens
templates/          # Jinja2 (Bootstrap/FA)
utils/              # Helpers (PDF, IO, parse, etc.)

app.py              # Entry-point Flask
config.py           # Configurações e .env
requirements.txt    # Dependências
README.md           # Este arquivo
Documentação do Sistema.docx  # Documento de referência interna
LICENSE             # Apache-2.0
```

---

## ⚙️ Requisitos

- **Python 3.10+**
- Windows ou Linux
- (Opcional) **Ghostscript** + **`camelot-py[cv]`** para PDFs/tabelas difíceis.
- (Opcional) **Cloudflare Tunnel** para expor o app com HTTPS e ocultar o IP.

### Dependências (via `requirements.txt`)
Principais libs (ajuste conforme seu arquivo):
```
Flask
pandas
numpy
pdfplumber
camelot-py[cv]    # opcional
openpyxl
matplotlib
python-dotenv
Werkzeug
```

---

## 🚀 Como rodar (dev)

```bash
# 1) Clonar
git clone https://github.com/Widson64x/Aereo-Comparativos.git
cd Aereo-Comparativos

# 2) Ambiente virtual
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

# 3) Instalar deps
pip install -r requirements.txt

# 4) Variáveis de ambiente
cp .env.example .env  # edite os valores conforme abaixo

# 5) Subir o app
python app.py
# ou
flask --app app run --debug
```

### `.env` (exemplo)
```ini
FLASK_ENV=development
SECRET_KEY=troque-para-uma-chave-secreta
PORT=5000

# Diretórios
UPLOAD_DIR=uploads
OUTPUT_DIR=outputs
LOG_DIR=logs

# Extração/normalização
USE_CAMELOT=false
IGNORE_FIRST_SHEET=true
TZ=America/Sao_Paulo

# Regras
TOLERANCIA_PERCENT=0.05
FRETE_MINIMO=60
```

---

## 🧪 Fluxo de uso

1) **Upload de Fatura (PDF)** → extração, cálculo de **`Frete Peso`** e pré-visualização.  
2) **Upload de Acordos (XLSX)** → leitura priorizando **Reservado LUFT - Bases** (ou demais abas com `Origem/Destino/Valor/Tratamento`), geração de **`Tipo_Serviço`**, renomeações e limpeza para export.  
3) **Download** do Excel processado e **Dashboards** com filtros.

> Em produção: persistir resultados em banco (PostgreSQL, por exemplo) e registrar logs estruturados.

---

## 🧯 Troubleshooting (erros comuns)

- **`Invalid Excel character '[]:*?/\` in sheetname 'MD/PE'`** → não use valores de **Tratamento** como *sheet name*; use como **valor de coluna**.  
- **`'DataFrame' object has no attribute 'str'`** → `.str` só existe para **Series de texto**; converta (`astype(str)`) ou use `pd.to_numeric(..., errors='coerce')` para números.  
- **`Nenhuma aba com dados válidos (Origem, Destino, Valor)`** → verifique `IGNORE_FIRST_SHEET` e se as abas trazem essas colunas.

---

## 🧱 Convenções de dados

- **Fatura**: exige mapeamento/normalização de nomes de colunas; acentos e variações são tratados.
- **Acordos**:
  - **Prioridade** da aba `Reservado LUFT - Bases`; senão, varrer demais abas com `Origem/Destino/Valor/Tratamento`.
  - **`Tipo_Serviço`** mapeado a partir de rótulos como `RESMD`, `ST2MD`, `ST2PE`, `ST2BA`, `MEDICAMENTOS`, `MD/PE`, `MD` etc.
  - **Export** com renomeações (`Documento → nOca`, `Frete_Acordo → Frete_Tabela`) e remoções (`Dif_abs`, `Observacao`).

---

## 📦 Deploy (resumo)

- **Windows**: `pythonw.exe` + **Task Scheduler** (início de máquina); logs em `LOG_DIR` (ex.: `.\logs` ou `\\172.16.200.80\c$\ProjetosPython\Aéreo\logs`).  
- **Linux**: `gunicorn -w 3 -b 127.0.0.1:5000 "app:create_app()"` atrás de Nginx, ou via **Cloudflare Tunnel**.

---

## 📚 Documentação & Licença

- Documentação adicional: `Documentação do Sistema.docx` (na raiz do repo).  
- Licença: **Apache-2.0** (`LICENSE`).

---

## 🤝 Contribuição

1. Crie sua branch: `git checkout -b feat/minha-feature`  
2. Commit: `git commit -m "feat: descrição"`  
3. Push: `git push origin feat/minha-feature`  
4. Abra um **Pull Request** 🎯

---

> **Créditos**: Equipe LUFT e usuários internos pelo feedback; comunidade open-source (Flask, Pandas, pdfplumber, Camelot, etc.).
