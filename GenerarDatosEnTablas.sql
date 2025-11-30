USE DW_FinanzasVentas;
GO

-- Borrar datos existentes de manera segura
DELETE FROM DimTiempo;
GO

DECLARE 
    @FechaInicio DATE = '2015-01-01',
    @FechaFin DATE = '2035-12-31';

WHILE (@FechaInicio <= @FechaFin)
BEGIN
    INSERT INTO DimTiempo (
        Fecha,
        Anio,
        Mes,
        NombreMes,
        Trimestre,
        Dia,
        DiaSemanaNumero,
        NombreDiaSemana
    )
    VALUES (
        @FechaInicio,
        YEAR(@FechaInicio),
        MONTH(@FechaInicio),
        DATENAME(MONTH, @FechaInicio),
        DATEPART(QUARTER, @FechaInicio),
        DAY(@FechaInicio),
        DATEPART(WEEKDAY, @FechaInicio),
        DATENAME(WEEKDAY, @FechaInicio)
    );

    SET @FechaInicio = DATEADD(DAY, 1, @FechaInicio);
END;

SELECT TOP 50 * FROM DimTiempo;

-- Crear tabla de números (1 a 50,000)
IF OBJECT_ID('tempdb..#Nums') IS NOT NULL DROP TABLE #Nums;

WITH N AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM N WHERE n < 50000
)
SELECT n INTO #Nums FROM N OPTION (MAXRECURSION 0);

-- Limpiar la tabla
DELETE FROM DimCliente;

INSERT INTO DimCliente (IdCliente, Nombre, Segmento, Region, FechaRegistro)
SELECT TOP 300
    n AS IdCliente,
    CONCAT('Cliente ', n) AS Nombre,
    CASE (n % 3)
        WHEN 0 THEN 'Minorista'
        WHEN 1 THEN 'Mayorista'
        ELSE 'Corporativo'
    END AS Segmento,
    CASE (n % 4)
        WHEN 0 THEN 'Centro'
        WHEN 1 THEN 'Occidente'
        WHEN 2 THEN 'Oriente'
        ELSE 'Norte'
    END AS Region,
    DATEADD(DAY, -n, GETDATE()) AS FechaRegistro
FROM #Nums
ORDER BY n;

DELETE FROM DimProducto;

INSERT INTO DimProducto (IdProducto, Categoria, Subcategoria, CostoUnitario, MargenBeneficio)
SELECT TOP 60
    n AS IdProducto,
    CASE (n % 4)
        WHEN 0 THEN 'Electrónica'
        WHEN 1 THEN 'Hogar'
        WHEN 2 THEN 'Ropa'
        ELSE 'Oficina'
    END AS Categoria,
    CASE (n % 4)
        WHEN 0 THEN 'Premium'
        WHEN 1 THEN 'Standard'
        WHEN 2 THEN 'Básico'
        ELSE 'Económico'
    END AS Subcategoria,
    (n % 200) + 5 AS CostoUnitario,
    (n % 25) + 5 AS MargenBeneficio
FROM #Nums
ORDER BY n;

DELETE FROM DimGasto;

INSERT INTO DimGasto (IdGasto, CategoriaGasto, Departamento)
SELECT TOP 200
    n AS IdGasto,
    CASE (n % 4)
        WHEN 0 THEN 'Nómina'
        WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'Operativos'
        ELSE 'Logística'
    END AS CategoriaGasto,
    CASE (n % 4)
        WHEN 0 THEN 'Finanzas'
        WHEN 1 THEN 'Ventas'
        WHEN 2 THEN 'RRHH'
        ELSE 'Gerencia'
    END AS Departamento
FROM #Nums
ORDER BY n;

DELETE FROM FactVentas;

INSERT INTO FactVentas (IdTiempo, IdCliente, IdProducto, Cantidad, IngresoBruto, CostoTotal, Utilidad, Estado)
SELECT TOP 20000
    (SELECT TOP 1 IdTiempo FROM DimTiempo ORDER BY NEWID()) AS IdTiempo,
    (SELECT TOP 1 IdCliente FROM DimCliente ORDER BY NEWID()) AS IdCliente,
    (SELECT TOP 1 IdProducto FROM DimProducto ORDER BY NEWID()) AS IdProducto,
    (n % 10) + 1 AS Cantidad,
    0 AS IngresoBruto,
    0 AS CostoTotal,
    0 AS Utilidad,
    CASE (n % 10)
        WHEN 0 THEN 'Cancelado'
        WHEN 1 THEN 'Devuelto'
        ELSE 'Activo'
    END AS Estado
FROM #Nums
ORDER BY n;

UPDATE F
SET 
    IngresoBruto = F.Cantidad * (P.CostoUnitario * (1 + (P.MargenBeneficio / 100.0))),
    CostoTotal = F.Cantidad * P.CostoUnitario,
    Utilidad = (F.Cantidad * (P.CostoUnitario * (1 + (P.MargenBeneficio / 100.0))))
               - (F.Cantidad * P.CostoUnitario)
FROM FactVentas F
JOIN DimProducto P ON F.IdProducto = P.IdProducto;

DELETE FROM FactGastos;

INSERT INTO FactGastos (IdGasto, IdTiempo, Monto, TipoGasto)
SELECT TOP 3000
    (SELECT TOP 1 IdGasto FROM DimGasto ORDER BY NEWID()) AS IdGasto,
    (SELECT TOP 1 IdTiempo FROM DimTiempo ORDER BY NEWID()) AS IdTiempo,
    (n % 5000) + 50 AS Monto,
    CategoriaGasto
FROM DimGasto D
JOIN #Nums N ON 1 = 1
ORDER BY N.n;

