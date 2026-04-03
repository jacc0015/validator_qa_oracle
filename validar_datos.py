import oracledb
import yaml
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class BlackboardValidator:
    def __init__(self):
        try:
            user, password, dsn = os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_DSN")
            self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
            self.engine = create_engine(f"oracle+oracledb://{user}:{password}@{dsn}")
            
            # Carpeta única por ejecución
            self.folder_run = f"logs/run_{datetime.now().strftime('%Y%m%d_%H%M')}"
            if not os.path.exists(self.folder_run): os.makedirs(self.folder_run)
            
            print(f"✅ Auditoría Iniciada en: {self.folder_run}")
        except Exception as e:
            print(f"❌ Error de Conexión: {e}"); exit()

    def ejecutar_auditoria(self):
        with open("config/queries.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        resumen_final = []

        for p in config['procesos']:
            print(f"\n🚀 Procesando: {p['nombre']}...")
            key = p.get('key')

            # 1. Extracción de Datos a Memoria y Archivo
            df_src = pd.read_sql(f"SELECT * FROM {p['vista_origen']}", self.engine)
            df_dst = pd.read_sql(f"SELECT * FROM {p['tabla_destino']}", self.engine)

            df_src.to_csv(f"{self.folder_run}/SRC_{p['nombre']}.csv", index=False)
            df_dst.to_csv(f"{self.folder_run}/DST_{p['nombre']}.csv", index=False)

            # 2. Comparación Lógica (Simulando MINUS de Oracle en Pandas)
            # Buscamos registros en SRC que no coincidan exactamente en DST
            df_all = df_src.merge(df_dst, how='left', indicator=True)
            df_diff = df_all[df_all['_merge'] == 'left_only'].drop('_merge', axis=1)

            cnt_v, cnt_t = len(df_src), len(df_dst)
            diffs = len(df_diff)

            # --- GESTIÓN DE STATUS ---
            if diffs == 0 and cnt_v == cnt_t:
                status = "EXITO"
            elif diffs == 0 and cnt_v != cnt_t:
                status = "WARN (DUPL)" # Los datos están, pero las cantidades no calzan
            else:
                status = "FALLO"

            # 3. Guardar errores si existen
            if diffs > 0:
                if key and key in df_diff.columns:
                    df_diff = df_diff.sort_values(by=key)
                df_diff.to_csv(f"{self.folder_run}/ERR_{p['nombre']}.csv", index=False)
                print(f"   ❌ Status: {status} ({diffs} discrepancias)")
            else:
                print(f"   ✅ Status: {status}")

            resumen_final.append({
                "PROCESO": p['nombre'],
                "STATUS": status,
                "VISTA": cnt_v,
                "TABLA": cnt_t,
                "DIFF": diffs
            })

        self._imprimir_resumen(resumen_final)
        self.conn.close()

    def _imprimir_resumen(self, datos):
        print("\n" + "="*95)
        print(f"{'PROCESO':<35} | {'STATUS':<12} | {'VISTA':<12} | {'TABLA':<12} | {'DIFF':<6}")
        print("-" * 95)
        for d in datos:
            print(f"{d['PROCESO'][:34]:<35} | {d['STATUS']:<12} | {d['VISTA']:<12} | {d['TABLA']:<12} | {d['DIFF']:<6}")
        print("="*95)
        print(f"📂 Archivos detallados en: {self.folder_run}")

if __name__ == "__main__":
    BlackboardValidator().ejecutar_auditoria()