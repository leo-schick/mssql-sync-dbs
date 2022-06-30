CREATE FUNCTION GetDateFromIsoWeek(@Input VARCHAR(10))
RETURNS DATE
WITH EXECUTE AS CALLER
AS
BEGIN
    DECLARE @YearNum CHAR(4)
    DECLARE @WeekNum VARCHAR(2)
    declare @FirstDay datetime

    SET @YearNum = cast(SUBSTRING(@Input,0,CHARINDEX('W',@Input,0)) as int)
    IF @YearNum < 99
        SET @YearNum += 2000
    SET @WeekNum = SUBSTRING(@Input,CHARINDEX('W',@Input,0)+1,LEN(@Input))
    set @FirstDay=DATEADD(DAY, (@@DATEFIRST - DATEPART(WEEKDAY, DATEADD(YEAR, @YearNum - 1900, 0)) +  (8 - @@DATEFIRST) * 2) % 7, DATEADD(YEAR, @YearNum - 1900, 0))-1

    RETURN(DATEADD(wk, DATEDIFF(wk, 6, '1/1/' + @YearNum) + (@WeekNum-case when DATEDIFF ( day ,  convert(datetime,'01/01/'+ @YearNum),@FirstDay )>=3 then 1 else 0 end), 7));
END; 
GO
