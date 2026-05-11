import os, time
import pyarrow.parquet as pq
from datetime import datetime, timedelta

PARQUET   = r"C:\Users\ruan.feitosa\Desktop\Script\f_vendas.parquet"
TOTAL_EST = 44_355_940

inicio     = None
linhas_ini = 0
print("Monitorando exportação (Polars + Parquet)... (Ctrl+C para parar)\n")

while True:
    if os.path.exists(PARQUET):
        tam   = os.path.getsize(PARQUET) / 1024**2
        mtime = datetime.fromtimestamp(os.path.getmtime(PARQUET))

        try:
            meta   = pq.read_metadata(PARQUET)
            linhas = meta.num_rows
        except Exception:
            print(f"[{datetime.now():%H:%M:%S}] Parquet sendo gravado, aguardando...", flush=True)
            time.sleep(15)
            continue

        if inicio is None:
            inicio     = datetime.now()
            linhas_ini = linhas

        elapsed    = (datetime.now() - inicio).total_seconds()
        novas      = linhas - linhas_ini
        vel        = novas / elapsed if elapsed > 0 else 0
        restantes  = max(TOTAL_EST - linhas, 0)
        eta_s      = restantes / vel if vel > 0 else 0
        eta_hora   = (datetime.now() + timedelta(seconds=eta_s)).strftime("%H:%M")
        seg_parado = (datetime.now() - mtime).seconds

        status = "⏳ processando..." if seg_parado > 60 else "✅ gravando"

        print(f"[{datetime.now():%H:%M:%S}] "
              f"{tam:>7.1f} MB | "
              f"{linhas:>10,} linhas | "
              f"Vel: {vel:>7,.0f} lin/s | "
              f"ETA: {eta_hora} | "
              f"{status}",
              flush=True)
    else:
        print(f"[{datetime.now():%H:%M:%S}] Aguardando Parquet...", flush=True)

    time.sleep(15)
