import pandas as pd
import pyodbc
from config import SQL_SERVER, EXCEL_PATH


def get_sql_connection():
    conn_str = (
        f"DRIVER={SQL_SERVER['driver']};"
        f"SERVER={SQL_SERVER['server']};"
        f"DATABASE={SQL_SERVER['database']};"
        f"Trusted_Connection={SQL_SERVER['trusted_connection']};"
    )
    return pyodbc.connect(conn_str)


def export_to_excel():
    conn = get_sql_connection()

    print("Extrayendo datos desde SQL Server...")

    # Transacciones
    df_trans = pd.read_sql("""
        SELECT 
            T.Fecha,
            F.IdCliente AS ID_Cliente,
            F.IdProducto AS ID_Producto,
            F.Cantidad,
            P.CostoUnitario * (1 + P.MargenBeneficio / 100.0) AS Precio_Unitario,
            F.Estado
        FROM FactVentas F
        JOIN DimTiempo T ON F.IdTiempo = T.IdTiempo
        JOIN DimProducto P ON F.IdProducto = P.IdProducto;
    """, conn)

    # Clientes
    df_clientes = pd.read_sql("""
        SELECT 
            IdCliente AS ID_Cliente,
            Nombre,
            Segmento,
            Region AS Región,
            FechaRegistro AS Fecha_Registro
        FROM DimCliente;
    """, conn)

    # Productos
    df_productos = pd.read_sql("""
        SELECT 
            IdProducto AS ID_Producto,
            Categoria AS Categoría,
            Subcategoria AS Subcategoría,
            CostoUnitario AS Costo_Unitario,
            MargenBeneficio AS Margen_Beneficio
        FROM DimProducto;
    """, conn)

    # Gastos
    df_gastos = pd.read_sql("""
        SELECT 
            F.IdGasto AS ID_Gasto,
            T.Fecha,
            F.Monto,
            F.TipoGasto AS Categoría_Gasto
        FROM FactGastos F
        JOIN DimTiempo T ON F.IdTiempo = T.IdTiempo;
    """, conn)

    conn.close()

    print("Creando Excel...")

    with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
        df_trans.to_excel(writer, sheet_name='Transacciones', index=False)
        df_clientes.to_excel(writer, sheet_name='Clientes', index=False)
        df_productos.to_excel(writer, sheet_name='Productos', index=False)
        df_gastos.to_excel(writer, sheet_name='Gastos', index=False)

    print(f"Excel generado correctamente en: {EXCEL_PATH}")


if __name__ == "__main__":
    export_to_excel()
