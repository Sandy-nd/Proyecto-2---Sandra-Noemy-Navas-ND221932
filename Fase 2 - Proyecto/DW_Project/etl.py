import pandas as pd
import pyodbc
import datetime
from config import SQL_SERVER, EXCEL_PATH


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = "logs/etl_log.txt"

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - " + msg + "\n")
    print(msg)


# ============================================================
# CONEXIÓN SQL SERVER
# ============================================================

def get_sql_connection():
    conn_str = (
        f"DRIVER={SQL_SERVER['driver']};"
        f"SERVER={SQL_SERVER['server']};"
        f"DATABASE={SQL_SERVER['database']};"
        f"Trusted_Connection={SQL_SERVER['trusted_connection']};"
    )
    return pyodbc.connect(conn_str)


# ============================================================
# LIMPIEZA DE TABLAS (ORDEN CORRECTO)
# ============================================================

def clear_tables(conn):
    log("Limpiando tablas en orden correcto...")

    cursor = conn.cursor()

    # 1. Borrar tablas FACT primero
    cursor.execute("DELETE FROM FactGastos")
    cursor.execute("DELETE FROM FactVentas")

    # 2. Borrar tablas DIM después
    cursor.execute("DELETE FROM DimCliente")
    cursor.execute("DELETE FROM DimProducto")

    conn.commit()
    log("Tablas limpiadas correctamente.")


# ============================================================
# EXTRACT: LECTURA DEL EXCEL
# ============================================================

def extract_data():
    log("Leyendo Excel...")

    df_trans = pd.read_excel(EXCEL_PATH, sheet_name="Transacciones")
    df_cli   = pd.read_excel(EXCEL_PATH, sheet_name="Clientes")
    df_prod  = pd.read_excel(EXCEL_PATH, sheet_name="Productos")
    df_gast  = pd.read_excel(EXCEL_PATH, sheet_name="Gastos")

    log("Excel leído correctamente.")
    return df_trans, df_cli, df_prod, df_gast


# ============================================================
# TRANSFORM: CÁLCULOS + LIMPIEZA
# ============================================================

def transform_data(df_trans, df_cli, df_prod, df_gast):
    log("Transformando datos...")

    # Cálculos
    df_trans["IngresoBruto"] = df_trans["Cantidad"] * df_trans["Precio_Unitario"]

    costo_map = df_prod.set_index("ID_Producto")["Costo_Unitario"]
    df_trans["CostoTotal"] = df_trans["ID_Producto"].map(costo_map) * df_trans["Cantidad"]

    df_trans["Utilidad"] = df_trans["IngresoBruto"] - df_trans["CostoTotal"]

    # Limpieza
    df_trans.dropna(inplace=True)
    df_cli.dropna(inplace=True)
    df_prod.dropna(inplace=True)
    df_gast.dropna(inplace=True)

    log("Transformación completada.")
    return df_trans, df_cli, df_prod, df_gast


# ============================================================
# LOAD: CARGA A SQL SERVER
# ============================================================

def load_dim_cliente(conn, df_cli):
    log("Cargando DimCliente...")
    cursor = conn.cursor()

    for _, row in df_cli.iterrows():
        cursor.execute("""
            INSERT INTO DimCliente (IdCliente, Nombre, Segmento, Region, FechaRegistro)
            VALUES (?, ?, ?, ?, ?)
        """, row["ID_Cliente"], row["Nombre"], row["Segmento"],
             row["Región"], row["Fecha_Registro"])

    conn.commit()
    log("DimCliente cargada.")


def load_dim_producto(conn, df_prod):
    log("Cargando DimProducto...")
    cursor = conn.cursor()

    for _, row in df_prod.iterrows():
        cursor.execute("""
            INSERT INTO DimProducto (IdProducto, Categoria, Subcategoria, 
                                     CostoUnitario, MargenBeneficio)
            VALUES (?, ?, ?, ?, ?)
        """, row["ID_Producto"], row["Categoría"], row["Subcategoría"],
             row["Costo_Unitario"], row["Margen_Beneficio"])

    conn.commit()
    log("DimProducto cargada.")


def load_fact_ventas(conn, df_trans):
    log("Cargando FactVentas...")
    cursor = conn.cursor()

    for _, row in df_trans.iterrows():
        cursor.execute("""
            INSERT INTO FactVentas (IdTiempo, IdCliente, IdProducto,
                Cantidad, IngresoBruto, CostoTotal, Utilidad, Estado)
            VALUES (
                (SELECT IdTiempo FROM DimTiempo WHERE Fecha = ?),
                ?, ?, ?, ?, ?, ?, ?
            )
        """, row["Fecha"], row["ID_Cliente"], row["ID_Producto"],
             row["Cantidad"], row["IngresoBruto"], row["CostoTotal"],
             row["Utilidad"], row["Estado"])

    conn.commit()
    log("FactVentas cargada.")


def load_fact_gastos(conn, df_gast):
    log("Cargando FactGastos...")
    cursor = conn.cursor()

    for _, row in df_gast.iterrows():
        cursor.execute("""
            INSERT INTO FactGastos (IdGasto, IdTiempo, Monto, TipoGasto)
            VALUES (
                ?, 
                (SELECT IdTiempo FROM DimTiempo WHERE Fecha = ?),
                ?, ?
            )
        """, row["ID_Gasto"], row["Fecha"], row["Monto"], row["Categoría_Gasto"])

    conn.commit()
    log("FactGastos cargada.")


# ============================================================
# ORQUESTACIÓN: ETL COMPLETO
# ============================================================

def run_etl():
    log("===== INICIO ETL =====")

    conn = get_sql_connection()

    # 1. Limpiado en el orden correcto
    clear_tables(conn)

    # 2. Extract
    df_trans, df_cli, df_prod, df_gast = extract_data()

    # 3. Transform
    df_trans, df_cli, df_prod, df_gast = transform_data(df_trans, df_cli, df_prod, df_gast)

    # 4. Load Dimensions
    load_dim_cliente(conn, df_cli)
    load_dim_producto(conn, df_prod)

    # 5. Load Facts
    load_fact_ventas(conn, df_trans)
    load_fact_gastos(conn, df_gast)

    conn.close()
    log("===== ETL FINALIZADO =====")


if __name__ == "__main__":
    run_etl()
