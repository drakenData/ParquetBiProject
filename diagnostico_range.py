import sys, os, subprocess, re
sys.path.insert(0, r"C:\Users\ruan.feitosa\Desktop\Script\adomd_dlls")
os.add_dll_directory(r"C:\Users\ruan.feitosa\Desktop\Script\adomd_dlls")
from pyadomd import Pyadomd

def get_pbi_port():
    tasklist = subprocess.run(
        ['tasklist', '/FI', 'IMAGENAME eq msmdsrv.exe', '/FO', 'CSV', '/NH'],
        capture_output=True, text=True, encoding='cp850')
    import re
    pids = re.findall(r'"msmdsrv\.exe","(\d+)"', tasklist.stdout)
    netstat = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, encoding='cp850')
    for pid in pids:
        for linha in netstat.stdout.splitlines():
            if linha.strip().endswith(pid) and 'LISTEN' in linha.upper():
                m = re.search(r':(\d+)\s', linha)
                if m: return int(m.group(1))

port = get_pbi_port()
conn = Pyadomd(f"Provider=MSOLAP;Data Source=localhost:{port};")
conn.open()

cur = conn.cursor().execute("""
EVALUATE
ROW(
    "data_min", MINX(f_vendas , f_vendas [data_emissao]),
    "data_max", MAXX(f_vendas , f_vendas [data_emissao]),
    "total_linhas", COUNTROWS(f_vendas)
)
""")
row = cur.fetchall()[0]
print(f"Data mínima  : {row[0]}")
print(f"Data máxima  : {row[1]}")
print(f"Total linhas : {row[2]:,}")
cur.close()
conn.close()