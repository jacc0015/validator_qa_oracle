import oracledb
import yaml
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Cargar variables de entorno
load_dotenv()

class BlackboardValidator:
    def __init__(self):
        """Inicializa las conexiones optimizadas para Oracle 19c"""
        try:
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASS")
            dsn = os.getenv("DB_DSN")

            # Conexión nativa para ejecutar Stored Procedures
            self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
            
            # Motor SQLAlchemy para lectura eficiente con Pandas (Evita Warnings)
            self.engine = create_engine(f"oracle+oracledb://{user}:{password}@{dsn}")
            
            # Carpeta única por ejecución (Snapshot)
            self.folder_run = f"logs/run_{datetime.now().strftime('%Y%m%d_%H%M')}"
            if not os.path.exists(self.folder_run): 
                os.makedirs(self.folder_run)
            
            print(f"✅ Conexión establecida. Auditoría iniciada en: {self.folder_run}")
        except Exception as e:
            print(f"❌ Error crítico de conexión: {e}")
            exit()

    def ejecutar_auditoria(self):
        """Orquestador principal: Ejecuta SP, extrae espejos y valida integridad"""
        with open("config/queries.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        resumen_final = []

        for p in config['procesos']:
            print(f"\n" + "-"*60)
            print(f"🚀 PROCESO: {p['nombre']}")
            key_col = p.get('key')

            # --- A. EJECUCIÓN DEL STORED PROCEDURE ---
            if p.get('sp_name'):
                try:
                    print(f"⚙️  Llamando a SP: {p['sp_name']}...")
                    with self.conn.cursor() as cursor:
                        cursor.callproc(p['sp_name'])
                    print("   ✅ SP finalizado (Commit interno detectado).")
                except oracledb.DatabaseError as e:
                    error_obj, = e.args
                    print(f"   ❌ ERROR ORACLE: {error_obj.message}")
                except Exception as e:
                    print(f"   ❌ ERROR INESPERADO en SP: {e}")

            # --- B. EXTRACCIÓN DE "ESPEJOS" (SNAPSHOTS) ---
            print(f"📸 Generando archivos espejo (SRC vs DST)...")
            try:
                # Extraemos la Vista (Origen) y la Tabla (Destino)
                df_src = pd.read_sql(f"SELECT * FROM {p['vista_origen']}", self.engine)
                df_dst = pd.read_sql(f"SELECT * FROM {p['tabla_destino']}", self.engine)

                # Guardamos localmente para auditoría offline
                df_src.to_csv(f"{self.folder_run}/SRC_{p['nombre']}.csv", index=False)
                df_dst.to_csv(f"{self.folder_run}/DST_{p['nombre']}.csv", index=False)

                # --- C. COMPARACIÓN LÓGICA (AUDITORÍA) ---
                # Buscamos registros en SRC que no tengan un match exacto en DST
                df_all = df_src.merge(df_dst, how='left', indicator=True)
                df_diff = df_all[df_all['_merge'] == 'left_only'].drop('_merge', axis=1)

                cnt_v, cnt_t = len(df_src), len(df_dst)
                diffs = len(df_diff)

                # Gestión de Status
                if diffs == 0 and cnt_v == cnt_t:
                    status = "EXITO"
                elif diffs == 0 and cnt_v != cnt_t:
                    status = "WARN (DUPL)" # Data íntegra, pero cantidades distintas
                else:
                    status = "FALLO"

                # Guardar reporte de errores si existen
                if diffs > 0:
                    if key_col and key_col in df_diff.columns:
                        df_diff = df_diff.sort_values(by=key_col)
                    
                    err_path = f"{self.folder_run}/ERR_{p['nombre']}.csv"
                    df_diff.to_csv(err_path, index=False)
                    print(f"   🚩 STATUS: {status} ({diffs} discrepancias encontradas)")
                else:
                    print(f"   ✅ STATUS: {status}")

                resumen_final.append({
                    "PROCESO": p['nombre'],
                    "STATUS": status,
                    "VISTA": cnt_v,
                    "TABLA": cnt_t,
                    "DIFF": diffs
                })

            except Exception as e:
                print(f"   ❌ Error durante la validación de {p['nombre']}: {e}")

        # --- D. RESUMEN FINAL ---
        self._imprimir_resumen_visual(resumen_final)
        self.conn.close()
        self.engine.dispose()

    def _imprimir_resumen_visual(self, datos):
        """Muestra una tabla limpia en consola con los resultados"""
        print("\n" + "="*95)
        print(f"{'PROCESO':<35} | {'STATUS':<12} | {'VISTA':<12} | {'TABLA':<12} | {'DIFF':<6}")
        print("-" * 95)
        for d in datos:
            print(f"{d['PROCESO'][:34]:<35} | {d['STATUS']:<12} | {d['VISTA']:<12} | {d['TABLA']:<12} | {d['DIFF']:<6}")
        print("="*95)
        print(f"📂 Auditoría completa. Revisa los archivos en: {self.folder_run}\n")

if __name__ == "__main__":
    validator = BlackboardValidator()
    validator.ejecutar_auditoria()