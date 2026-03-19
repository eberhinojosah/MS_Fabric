CREATE   PROCEDURE sp_full_load
(
    @lh_name VARCHAR(50),
    @schema VARCHAR(50),
    @table VARCHAR(50)
)
AS
BEGIN

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @full_source NVARCHAR(MAX) = @lh_name + '.' + @schema + '.' + @table;

    IF OBJECT_ID(@table) IS NOT NULL
    BEGIN
        SET @sql = 'DROP TABLE ' + @table + ';';
        PRINT(@sql) --Mostrar el resultado por consola
        EXEC sp_executesql @sql;
    END

    SET @sql = '
        SELECT *
        INTO ' + @table + '
        FROM ' + @full_source + ';
    ';
    PRINT(@sql) --Mostrar el resultado por consola
    EXEC sp_executesql @sql;

END;