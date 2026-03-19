CREATE   PROCEDURE sp_incremental_load
(
    @lh_name VARCHAR(50),
    @schema VARCHAR(50),
    @table VARCHAR(50),
    @date VARCHAR(10) = NULL
)
AS
BEGIN

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @full_source NVARCHAR(MAX) = @lh_name + '.' + @schema + '.' + @table;

    IF @date IS NULL
    BEGIN
        SET @date = CAST(CAST(GETDATE() AS DATE) AS VARCHAR(10));
        PRINT(@date); --Mostrar el resultado por consola
    END;
    
    IF OBJECT_ID(@table) IS NOT NULL
    BEGIN

        SET @sql = 'DELETE FROM ' + @table + ' WHERE file_date = ''' + @date + ''';';
        PRINT(@sql) --Mostrar el resultado por consola
        EXEC sp_executesql @sql;
        
        SET @sql = '
            INSERT INTO ' + @table + '
            SELECT *
            FROM ' + @full_source + '
            WHERE file_date = ''' + @date +''';
        ';
        PRINT(@sql) --Mostrar el resultado por consola
        EXEC sp_executesql @sql;
        
    END
    ELSE
    BEGIN

        SET @sql = '
            SELECT *
            INTO ' + @table + '
            FROM ' + @full_source + ';
        ';
        PRINT(@sql) --Mostrar el resultado por consola
        EXEC sp_executesql @sql;

    END;
END;