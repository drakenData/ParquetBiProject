# exportar_vendas.py
import sys, os, subprocess, re, time, threading
from datetime import datetime, timedelta

# ── DLLs ─────────────────────────────────────────────────────
dll_path = r"C:\Users\ruan.feitosa\Desktop\Script\adomd_dlls"
sys.path.insert(0, dll_path)
os.add_dll_directory(dll_path)
os.environ["PATH"] = dll_path + ";" + os.environ["PATH"]

from pyadomd import Pyadomd
import polars as pl
import pyarrow.parquet as pq

# ── Config ────────────────────────────────────────────────────
OUTPUT_PARQUET = r"C:\Users\ruan.feitosa\Desktop\Script\f_vendas.parquet"
DIAS_CHUNK     = 4
MAX_WORKERS    = 2        
MAX_RETRIES    = 3
D_INI          = datetime(2025, 1, 2)
D_FIM          = datetime(2026, 4, 30)
TOTAL_EST      = 44_355_940

# ── Schema fixo ───────────────────────────────────────────────
SCHEMA_FIXO = {
    "codigo_cliente":  pl.Utf8,
    "vendedor":        pl.Utf8,
    "codigo_produto":  pl.Utf8,
    "valor_bruto":     pl.Float64,
    "valor_liquido":   pl.Float64,
    "unidade_vendida": pl.Float64,
    "data_emissao":    pl.Utf8,
    "nota_fiscal":     pl.Utf8,
    "tipo_nota":       pl.Utf8,
    "unidade":         pl.Utf8,
    "cod_canal":       pl.Utf8,
    "desc_finan":      pl.Float64,
    "apontador":       pl.Utf8,
    "empresa_cliente": pl.Utf8,
    "data_pedido":     pl.Utf8,
    "prazo_ponderado": pl.Float64,
    "pedido_venda":    pl.Utf8,
}

# ── Logger ────────────────────────────────────────────────────
_lock = threading.Lock()
def log(msg, nivel="INFO"):
    with _lock:
        print(f"[{datetime.now():%H:%M:%S}] [{nivel}] {msg}", flush=True)

# ── Porta ─────────────────────────────────────────────────────
def get_pbi_port():
    tasklist = subprocess.run(
        ['tasklist', '/FI', 'IMAGENAME eq msmdsrv.exe', '/FO', 'CSV', '/NH'],
        capture_output=True, text=True, encoding='cp850')
    pids = re.findall(r'"msmdsrv\.exe","(\d+)"', tasklist.stdout)
    if not pids: raise RuntimeError("Power BI não está aberto.")
    netstat = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, encoding='cp850')
    for pid in pids:
        for linha in netstat.stdout.splitlines():
            if linha.strip().endswith(pid) and 'LISTEN' in linha.upper():
                m = re.search(r':(\d+)\s', linha)
                if m: return int(m.group(1))
    raise RuntimeError("Porta não encontrada.")

PORT = get_pbi_port()
CONN = f"Provider=MSOLAP;Data Source=localhost:{PORT};"
log(f"Conectado na porta {PORT}")

# ── Controle seguro de escrita ────────────────────────────────
_pq_writer    = None
_schema_arrow = None

def gravar_parquet(df: pl.DataFrame):
    global _pq_writer, _schema_arrow
    table = df.to_arrow()
    if _pq_writer is None:
        _schema_arrow = table.schema
        _pq_writer = pq.ParquetWriter(
            OUTPUT_PARQUET,
            _schema_arrow,
            compression="snappy",
        )
    else:
        table = table.cast(_schema_arrow)
    _pq_writer.write_table(table)

def fechar_parquet():
    global _pq_writer, _schema_arrow
    if _pq_writer is not None:
        _pq_writer.close()
        _pq_writer    = None
        _schema_arrow = None

# ── Chunk com fetchall ────────────────────────────────────────
def exportar_chunk(d_ini, d_fim):
    query = f"""
    EVALUATE
    CALCULATETABLE(
        SELECTCOLUMNS(f_vendas,
            "codigo_cliente",  f_vendas[codigo_cliente],
            "vendedor",        f_vendas[vendedor],
            "codigo_produto",  f_vendas[codigo_produto],
            "valor_bruto",     f_vendas[valor_bruto],
            "valor_liquido",   f_vendas[valor_liquido],
            "unidade_vendida", f_vendas[unidade_vendida],
            "data_emissao",    f_vendas[data_emissao],
            "nota_fiscal",     f_vendas[nota_fiscal],
            "tipo_nota",       f_vendas[tipo_nota],
            "unidade",         f_vendas[unidade],
            "cod_canal",       f_vendas[cod_canal],
            "desc_finan",      f_vendas[desc_finan],
            "apontador",       f_vendas[apontador],
            "empresa_cliente", f_vendas[empresa_cliente],
            "data_pedido",     f_vendas[data_pedido],
            "prazo_ponderado", f_vendas[prazo_ponderado],
            "pedido_venda",    f_vendas[pedido_venda]
        ),
        f_vendas[data_emissao] >= DATE({d_ini.year},{d_ini.month},{d_ini.day}),
        f_vendas[data_emissao] <  DATE({d_fim.year},{d_fim.month},{d_fim.day})
    )"""

    t0    = time.time()
    total = 0

    conn = Pyadomd(CONN)
    conn.open()
    try:
        cur  = conn.cursor().execute(query)
        cols = [c.name.split('[')[-1].replace(']','') for c in cur.description]

        rows = cur.fetchall()
        if rows:
            df = pl.DataFrame(
                {col: [str(row[i]) if row[i] is not None else None for row in rows]
                 for i, col in enumerate(cols)},
                schema={col: pl.Utf8 for col in cols},
            )
            df = df.cast(SCHEMA_FIXO)
            gravar_parquet(df)
            total = len(df)

        cur.close()
    finally:
        conn.close()

    return total, time.time() - t0

def exportar_chunk_com_retry(d_ini, d_fim):
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            return exportar_chunk(d_ini, d_fim)
        except Exception as e:
            msg = str(e)[:120]
            if tentativa < MAX_RETRIES:
                log(f"    Tentativa {tentativa}/{MAX_RETRIES} falhou: {msg} — aguardando 10s...", "WARN")
                time.sleep(10)
            else:
                raise

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    chunks, cur = [], D_INI
    while cur < D_FIM:
        prox = min(cur + timedelta(days=DIAS_CHUNK), D_FIM)
        chunks.append((cur, prox))
        cur = prox

    log("=" * 62)
    log(f"EXPORTAÇÃO f_vendas — {len(chunks)} chunks de {DIAS_CHUNK} dia")
    log(f"17 colunas | fetchall por chunk | retry {MAX_RETRIES}x")
    log(f"Output: {OUTPUT_PARQUET}")
    log("=" * 62)

    if os.path.exists(OUTPUT_PARQUET):
        os.remove(OUTPUT_PARQUET)
        log("Parquet anterior removido — iniciando do zero.")

    t_inicio   = time.time()
    total_lido = 0
    erros      = []

    try:
        for i, (d_ini, d_fim) in enumerate(chunks, 1):
            label = f"{d_ini:%Y-%m-%d}→{d_fim:%Y-%m-%d}"
            try:
                n, dur = exportar_chunk_com_retry(d_ini, d_fim)
                total_lido += n
                vel = n / dur if dur > 0 else 0
                log(f"  [{i:>3}/{len(chunks)}] {label} | {n:>7,} linhas | "
                    f"{dur:.1f}s | {vel:,.0f} lin/s")

                if i % 10 == 0 or i == len(chunks):
                    elapsed = time.time() - t_inicio
                    pct     = total_lido / TOTAL_EST * 100
                    eta     = (elapsed / total_lido * (TOTAL_EST - total_lido)) if total_lido > 0 else 0
                    log(f"  ── {pct:.1f}% | {total_lido:,} linhas | "
                        f"Decorrido: {elapsed/60:.1f}min | ETA: {eta/60:.1f}min")

            except Exception as e:
                log(f"  ERRO {label}: {str(e)[:120]}", "ERRO")
                erros.append(label)
                time.sleep(2)

    finally:
        fechar_parquet()

    elapsed = time.time() - t_inicio
    tam     = os.path.getsize(OUTPUT_PARQUET) / 1024**2 if os.path.exists(OUTPUT_PARQUET) else 0
    log("=" * 62)
    log(f"✅ CONCLUÍDO! {total_lido:,} linhas | {tam:.0f} MB | {elapsed/60:.1f} min")
    if erros:
        log(f"❌ Erros ({len(erros)}): {erros}", "ERRO")