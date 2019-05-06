create_demographics_table_query = """
IF OBJECT_ID('dbo.Demographic', 'U') IS NOT NULL
DROP TABLE dbo.Demographic

CREATE TABLE dbo.Demographic
(
	ID INT
	, FirstName VARCHAR(255)
	, MiddleInitial VARCHAR(10)
	, LastName VARCHAR(255)
	, DOB DATETIME2
	, Sex CHAR(1)
	, FavoriteColor VARCHAR(255)
	, ProviderGroup VARCHAR(255)
	, FileDate DATE
)
"""

create_risk_table_query = """
IF OBJECT_ID('dbo.RiskByQuarter', 'U') IS NOT NULL
DROP TABLE dbo.RiskByQuarter

CREATE TABLE dbo.RiskByQuarter
(
	ID INT
	, Quarter CHAR(2)
	, AttributedFlag VARCHAR(10)
	, RiskScore NUMERIC(10,6)
	, FileDate DATE
)
"""

insert_demographics_query = """
INSERT INTO dbo.Demographic (
    ID
    , FirstName
    , MiddleInitial
    , LastName
    , DOB
    , Sex
    , FavoriteColor
    , ProviderGroup
    , FileDate
) VALUES (
    {0}
    , '{1}'
    , '{2}'
    , '{3}'
    , '{4}'
    , '{5}'
    , '{6}'
    , '{7}'
    , '{8}'
);
"""

insert_quarter_risk_query = """
INSERT INTO dbo.RiskByQuarter (
    ID
	, Quarter
	, AttributedFlag
	, RiskScore
	, FileDate
) VALUES (
    {0}
    , '{1}'
    , '{2}'
    , {3}
    , '{4}'
);
"""
