import oracledb
import yaml
import pandas as pd
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Cargar configuración
load_dotenv()

class BlackboardValidator:
    def __init__(self, use_cache=False):
        self.use_cache = use_cache
        self.start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            user, password, dsn = os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_DSN")
            self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
            self.engine = create_engine(f"oracle+oracledb://{user}:{password}@{dsn}")
            
            self.folder_run = f"logs/run_{datetime.now().strftime('%Y%m%d_%H%M')}"
            self.folder_master = "master_data"
            
            for f in [self.folder_run, self.folder_master]:
                if not os.path.exists(f): os.makedirs(f)
            
            print(f"🚀 Auditoría Iniciada: {self.start_time} | Modo Caché: {'SÍ' if self.use_cache else 'NO'}")
        except Exception as e:
            print(f"❌ Error crítico: {e}"); sys.exit(1)

    def ejecutar_auditoria(self):
        with open("config/queries.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        resumen_final = []

        for p in config['procesos']:
            hora_p = datetime.now().strftime('%H:%M:%S')
            print(f"\n" + "="*80 + f"\n📦 PROCESO: {p['nombre']} | 🕒 {hora_p}")
            
            master_file = f"{self.folder_master}/SRC_{p['nombre']}.csv"
            key_col = p.get('key')

            # --- A. STORED PROCEDURE ---
            if p.get('sp_name'):
                try:
                    with self.conn.cursor() as cursor:
                        cursor.callproc(p['sp_name'])
                    print("   ✅ SP finalizado correctamente.")
                except Exception as e:
                    print(f"   ⚠️ Error en SP: {e}")

            # --- B. EXTRACCIÓN Y LOGS ---
            try:
                if self.use_cache and os.path.exists(master_file):
                    df_src = pd.read_csv(master_file)
                    df_src.to_csv(f"{self.folder_run}/SRC_{p['nombre']}.csv", index=False)
                else:
                    df_src = pd.read_sql(f"SELECT * FROM {p['vista_origen']}", self.engine)
                    df_src.to_csv(master_file, index=False)
                    df_src.to_csv(f"{self.folder_run}/SRC_{p['nombre']}.csv", index=False)

                df_dst = pd.read_sql(f"SELECT * FROM {p['tabla_destino']}", self.engine)
                df_dst.to_csv(f"{self.folder_run}/DST_{p['nombre']}.csv", index=False)

                # --- C. HASHING CON NORMALIZACIÓN DE ESPEJO ---
                def generar_hashes(df):
                    if df.empty: return pd.Series(dtype='uint64')
                    # 1. Alineamos columnas A-Z para que SRC y DST coincidan en estructura
                    temp = df.reindex(sorted(df.columns), axis=1).copy()
                    # 2. Limpieza total: strip de espacios y homologación de nulos
                    for col in temp.columns:
                        temp[col] = temp[col].astype(str).str.strip().replace(['None', 'nan', 'NaN', 'null', 'NULL'], '')
                    # 3. Firma digital
                    return pd.util.hash_pandas_object(temp, index=False)

                print(f"   ⚡ Normalizando y comparando registros...")
                df_src['row_hash'] = generar_hashes(df_src)
                df_dst['row_hash'] = generar_hashes(df_dst)

                # Identificar discrepancias (Lo que está en origen pero no en destino)
                df_diff = df_src[~df_src['row_hash'].isin(df_dst['row_hash'])].copy()
                
                df_src.drop(columns=['row_hash'], inplace=True)
                df_dst.drop(columns=['row_hash'], inplace=True)

                cnt_v, cnt_t, diffs = len(df_src), len(df_dst), len(df_diff)
                status = "EXITO" if (diffs == 0 and cnt_v == cnt_t) else ("WARN (DUPL)" if diffs == 0 else "FALLO")

                if diffs > 0:
                    if key_col and key_col in df_diff.columns:
                        df_diff = df_diff.sort_values(by=key_col)
                    df_diff.drop(columns=['row_hash'], inplace=True)
                    df_diff.to_csv(f"{self.folder_run}/ERR_{p['nombre']}.csv", index=False)
                    print(f"   🚩 STATUS: {status} ({diffs} diferencias)")
                else:
                    print(f"   ✅ STATUS: {status}")

                resumen_final.append({
                    "HORA": hora_p, "PROCESO": p['nombre'], "STATUS": status,
                    "VISTA": cnt_v, "TABLA": cnt_t, "DIFF": diffs
                })

            except Exception as e:
                print(f"   ❌ Error fatal: {e}")

        self._imprimir_resumen(resumen_final)
        self.conn.close()

    def _imprimir_resumen(self, datos):
        print("\n" + "="*115)
        print(f"{'HORA':<10} | {'PROCESO':<35} | {'STATUS':<12} | {'VISTA':<12} | {'TABLA':<12} | {'DIFF':<6}")
        print("-" * 115)
        for d in datos:
            print(f"{d['HORA']:<10} | {d['PROCESO'][:34]:<35} | {d['STATUS']:<12} | {d['VISTA']:<12} | {d['TABLA']:<12} | {d['DIFF']:<6}")
        print("="*115 + f"\n📂 Logs en: {self.folder_run}\n")

if __name__ == "__main__":
    cache_mode = "--use-cache" in sys.argv
    BlackboardValidator(use_cache=cache_mode).ejecutar_auditoria()