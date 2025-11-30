import pandas as pd
import pyodbc
import datetime
from config import SQL_SERVER, EXCEL_PATH

LOG_FILE = "logs/etl_incremental_log.txt"


# ============================================================
# LOG
# ============================================================

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - {msg}\n")
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
# EXTRACT
# ============================================================

def extract_data():
    log("Leyendo Excel (incremental)...")

    df_trans = pd.read_excel(EXCEL_PATH, sheet_name="Transacciones")
    df_cli   = pd.read_excel(EXCEL_PATH, sheet_name="Clientes")
    df_prod  = pd.read_excel(EXCEL_PATH, sheet_name="Productos")
    df_gast  = pd.read_excel(EXCEL_PATH, sheet_name="Gastos")

    log("Excel leído correctamente (incremental).")
    return df_trans, df_cli, df_prod, df_gast


# ============================================================
# TRANSFORM — (CONVERSIÓN DE FECHAS ARREGLADA)
# ============================================================

def transform_data(df_trans, df_cli, df_prod, df_gast):
    log("Transformando datos (incremental)...")

    # --- Conversión correcta de fechas a tipo date ---
    df_trans["Fecha"] = pd.to_datetime(df_trans["Fecha"]).dt.date
    df_gast["Fecha"]  = pd.to_datetime(df_gast["Fecha"]).dt.date

    # --- Cálculos ---
    df_trans["IngresoBruto"] = df_trans["Cantidad"] * df_trans["Precio_Unitario"]

    costo_map = df_prod.set_index("ID_Producto")["Costo_Unitario"]
    df_trans["CostoTotal"] = df_trans["ID_Producto"].map(costo_map) * df_trans["Cantidad"]

    df_trans["Utilidad"] = df_trans["IngresoBruto"] - df_trans["CostoTotal"]

    # Limpieza general
    df_trans.dropna(inplace=True)
    df_cli.dropna(inplace=True)
    df_prod.dropna(inplace=True)
    df_gast.dropna(inplace=True)

    log("Transformación incremental completada.")
    return df_trans, df_cli, df_prod, df_gast


# ============================================================
# OBTENER ÚLTIMAS FECHAS EN EL DW
# ============================================================

def get_max_dates(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT MAX(T.Fecha)
        FROM FactVentas F
        JOIN DimTiempo T ON F.IdTiempo = T.IdTiempo;
    """)
    max_fecha_ventas = cursor.fetchone()[0]

    cursor.execute("""
        SELECT MAX(T.Fecha)
        FROM FactGastos F
        JOIN DimTiempo T ON F.IdTiempo = T.IdTiempo;
    """)
    max_fecha_gastos = cursor.fetchone()[0]

    log(f"Última fecha en FactVentas: {max_fecha_ventas}")
    log(f"Última fecha en FactGastos: {max_fecha_gastos}")

    return max_fecha_ventas, max_fecha_gastos


# ============================================================
# FILTRO INCREMENTAL — ¡CORREGIDO!
# ============================================================

def filter_incremental(df_trans, df_gast, max_fecha_ventas, max_fecha_gastos):

    if max_fecha_ventas:
        max_fecha_ventas = pd.to_datetime(max_fecha_ventas).date()
        df_trans_inc = df_trans[df_trans["Fecha"] > max_fecha_ventas]
    else:
        df_trans_inc = df_trans

    if max_fecha_gastos:
        max_fecha_gastos = pd.to_datetime(max_fecha_gastos).date()
        df_gast_inc = df_gast[df_gast["Fecha"] > max_fecha_gastos]
    else:
        df_gast_inc = df_gast

    log(f"Transacciones nuevas: {len(df_trans_inc)}")
    log(f"Gastos nuevos: {len(df_gast_inc)}")

    return df_trans_inc, df_gast_inc


# ============================================================
# UPSERT DE DIMENSIONES (TYPE 1)
# ============================================================

def upsert_dim_cliente(conn, df_cli):
    log("Actualizando/insertando DimCliente (incremental)...")
    cursor = conn.cursor()

    for _, row in df_cli.iterrows():
        cursor.execute("""
            UPDATE DimCliente
            SET Nombre = ?, Segmento = ?, Region = ?, FechaRegistro = ?
            WHERE IdCliente = ?;
        """, row["Nombre"], row["Segmento"], row["Región"], row["Fecha_Registro"], row["ID_Cliente"])

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO DimCliente (IdCliente, Nombre, Segmento, Region, FechaRegistro)
                VALUES (?, ?, ?, ?, ?);
            """, row["ID_Cliente"], row["Nombre"], row["Segmento"], row["Región"], row["Fecha_Registro"])

    conn.commit()
    log("DimCliente actualizada (incremental).")


def upsert_dim_producto(conn, df_prod):
    log("Actualizando/insertando DimProducto (incremental)...")
    cursor = conn.cursor()

    for _, row in df_prod.iterrows():
        cursor.execute("""
            UPDATE DimProducto
            SET Categoria = ?, Subcategoria = ?, CostoUnitario = ?, MargenBeneficio = ?
            WHERE IdProducto = ?;
        """, row["Categoría"], row["Subcategoría"], row["Costo_Unitario"],
             row["Margen_Beneficio"], row["ID_Producto"])

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO DimProducto (IdProducto, Categoria, Subcategoria, CostoUnitario, MargenBeneficio)
                VALUES (?, ?, ?, ?, ?);
            """, row["ID_Producto"], row["Categoría"], row["Subcategoría"],
                 row["Costo_Unitario"], row["Margen_Beneficio"])

    conn.commit()
    log("DimProducto actualizada (incremental).")


# ============================================================
# CARGA INCREMENTAL FACTVENTAS
# ============================================================

def load_fact_ventas_incremental(conn, df_trans_inc):
    log("Cargando FactVentas (incremental)...")
    cursor = conn.cursor()

    inserted = 0

    for _, row in df_trans_inc.iterrows():
        cursor.execute("""
            INSERT INTO FactVentas (IdTiempo, IdCliente, IdProducto,
                Cantidad, IngresoBruto, CostoTotal, Utilidad, Estado)
            VALUES (
                (SELECT IdTiempo FROM DimTiempo WHERE Fecha = ?),
                ?, ?, ?, ?, ?, ?, ?
            )
        """, row["Fecha"], row["ID_Cliente"], row["ID_Producto"], row["Cantidad"],
             row["IngresoBruto"], row["CostoTotal"], row["Utilidad"], row["Estado"])
        inserted += 1

    conn.commit()
    log(f"FactVentas incremental cargada. Filas insertadas: {inserted}")


# ============================================================
# CARGA INCREMENTAL FACTGASTOS
# ============================================================

def load_fact_gastos_incremental(conn, df_gast_inc):
    log("Cargando FactGastos (incremental)...")
    cursor = conn.cursor()

    inserted = 0

    for _, row in df_gast_inc.iterrows():
        cursor.execute("""
            INSERT INTO FactGastos (IdGasto, IdTiempo, Monto, TipoGasto)
            VALUES (
                ?, 
                (SELECT IdTiempo FROM DimTiempo WHERE Fecha = ?),
                ?, ?
            )
        """, row["ID_Gasto"], row["Fecha"], row["Monto"], row["Categoría_Gasto"])
        inserted += 1

    conn.commit()
    log(f"FactGastos incremental cargada. Filas insertadas: {inserted}")


# ============================================================
# ORQUESTADOR INCREMENTAL
# ============================================================

def run_etl_incremental():
    log("===== INICIO ETL INCREMENTAL =====")

    conn = get_sql_connection()

    df_trans, df_cli, df_prod, df_gast = extract_data()

    df_trans, df_cli, df_prod, df_gast = transform_data(df_trans, df_cli, df_prod, df_gast)

    max_fecha_ventas, max_fecha_gastos = get_max_dates(conn)

    df_trans_inc, df_gast_inc = filter_incremental(df_trans, df_gast, max_fecha_ventas, max_fecha_gastos)

    upsert_dim_cliente(conn, df_cli)
    upsert_dim_producto(conn, df_prod)

    if len(df_trans_inc) > 0:
        load_fact_ventas_incremental(conn, df_trans_inc)
    else:
        log("No hay nuevas transacciones para cargar en FactVentas.")

    if len(df_gast_inc) > 0:
        load_fact_gastos_incremental(conn, df_gast_inc)
    else:
        log("No hay nuevos gastos para cargar en FactGastos.")

    conn.close()
    log("===== FIN ETL INCREMENTAL =====")


if __name__ == "__main__":
    run_etl_incremental()
