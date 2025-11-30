/* ===========================================
   1. Crear la base de datos si no existe
   =========================================== */
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_FinanzasVentas')
BEGIN
    CREATE DATABASE DW_FinanzasVentas;
END
GO

USE DW_FinanzasVentas;
GO

/* ===========================================
   DIMENSIONES
   =========================================== */

/* Dimensión Tiempo */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'DimTiempo') AND type = 'U')
BEGIN
    CREATE TABLE DimTiempo (
        IdTiempo INT IDENTITY(1,1) PRIMARY KEY,
        Fecha DATE NOT NULL UNIQUE,
        Anio INT NOT NULL,
        Mes INT NOT NULL,
        NombreMes NVARCHAR(20) NOT NULL,
        Trimestre INT NOT NULL,
        Dia INT NOT NULL,
        DiaSemanaNumero INT NOT NULL,
        NombreDiaSemana NVARCHAR(20) NOT NULL
    );
END
GO

/* Dimensión Cliente */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'DimCliente') AND type = 'U')
BEGIN
    CREATE TABLE DimCliente (
        IdCliente INT NOT NULL PRIMARY KEY,
        Nombre NVARCHAR(150) NOT NULL,
        Segmento NVARCHAR(50) NOT NULL,
        Region NVARCHAR(50) NOT NULL,
        FechaRegistro DATE NULL
    );
END
GO

/* Dimensión Producto */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'DimProducto') AND type = 'U')
BEGIN
    CREATE TABLE DimProducto (
        IdProducto INT NOT NULL PRIMARY KEY,
        Categoria NVARCHAR(100) NOT NULL,
        Subcategoria NVARCHAR(100) NOT NULL,
        CostoUnitario DECIMAL(18,2) NOT NULL,
        MargenBeneficio DECIMAL(5,2) NULL
    );
END
GO

/* Dimensión Gasto */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'DimGasto') AND type = 'U')
BEGIN
    CREATE TABLE DimGasto (
        IdGasto INT NOT NULL PRIMARY KEY,
        CategoriaGasto NVARCHAR(100) NOT NULL,
        Departamento NVARCHAR(100) NOT NULL
    );
END
GO

/* ===========================================
   TABLAS DE HECHOS
   =========================================== */

/* Hechos de Ventas */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'FactVentas') AND type = 'U')
BEGIN
    CREATE TABLE FactVentas (
        IdVenta BIGINT IDENTITY(1,1) PRIMARY KEY,
        IdTiempo INT NOT NULL,
        IdCliente INT NOT NULL,
        IdProducto INT NOT NULL,
        Cantidad INT NOT NULL,
        IngresoBruto DECIMAL(18,2) NOT NULL,
        CostoTotal DECIMAL(18,2) NOT NULL,
        Utilidad DECIMAL(18,2) NOT NULL,
        Estado NVARCHAR(20) NULL,
        CONSTRAINT FK_FactVentas_DimTiempo FOREIGN KEY (IdTiempo) REFERENCES DimTiempo(IdTiempo),
        CONSTRAINT FK_FactVentas_DimCliente FOREIGN KEY (IdCliente) REFERENCES DimCliente(IdCliente),
        CONSTRAINT FK_FactVentas_DimProducto FOREIGN KEY (IdProducto) REFERENCES DimProducto(IdProducto)
    );
END
GO

/* Hechos de Gastos */
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'FactGastos') AND type = 'U')
BEGIN
    CREATE TABLE FactGastos (
        IdFactGasto BIGINT IDENTITY(1,1) PRIMARY KEY,
        IdGasto INT NOT NULL,
        IdTiempo INT NOT NULL,
        Monto DECIMAL(18,2) NOT NULL,
        TipoGasto NVARCHAR(100) NULL,
        CONSTRAINT FK_FactGastos_DimGasto FOREIGN KEY (IdGasto) REFERENCES DimGasto(IdGasto),
        CONSTRAINT FK_FactGastos_DimTiempo FOREIGN KEY (IdTiempo) REFERENCES DimTiempo(IdTiempo)
    );
END
GO

/* ===========================================
   ÍNDICES PARA RENDIMIENTO
   =========================================== */

/* Índices de FactVentas */
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactVentas_IdTiempo' AND object_id = OBJECT_ID('FactVentas'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_IdTiempo ON FactVentas(IdTiempo);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactVentas_IdCliente' AND object_id = OBJECT_ID('FactVentas'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_IdCliente ON FactVentas(IdCliente);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactVentas_IdProducto' AND object_id = OBJECT_ID('FactVentas'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_IdProducto ON FactVentas(IdProducto);
END
GO

/* Índices de FactGastos */
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactGastos_IdTiempo' AND object_id = OBJECT_ID('FactGastos'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactGastos_IdTiempo ON FactGastos(IdTiempo);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactGastos_IdGasto' AND object_id = OBJECT_ID('FactGastos'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactGastos_IdGasto ON FactGastos(IdGasto);
END
GO
