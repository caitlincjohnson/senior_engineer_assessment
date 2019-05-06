USE PERSONDATABASE

/*********************
Hello! 

Please use the test data provided in the file 'PersonDatabase' to answer the following
questions. Please also import the dbo.Contacts flat file to a table for use. 

All answers should be executable on a MS SQL Server 2012 instance. 

***********************



QUESTION 1

The table dbo.Risk contains calculated risk scores for the population in dbo.Person. Write a 
query or group of queries that return the patient name, and their most recent risk level(s). 
Any patients that dont have a risk level should also be included in the results. 

**********************/

SELECT 
    PersonName
    ,r3.risk RiskLevel
FROM dbo.Person p
LEFT JOIN (
SELECT r1.PersonID id, r2.MaxRiskDateTime, r1.RiskLevel risk
FROM dbo.Risk r1
INNER JOIN (
    SELECT
        PersonID
        ,MAX(RiskDateTime) MaxRiskDateTime
    FROM dbo.Risk
    GROUP BY PersonID
) r2
ON r1.PersonID = r2.PersonID
AND r1.RiskDateTime = r2.MaxRiskDateTime
) r3
ON p.PersonID = r3.id

/**********************

QUESTION 2


The table dbo.Person contains basic demographic information. The source system users 
input nicknames as strings inside parenthesis. Write a query or group of queries to 
return the full name and nickname of each person. The nickname should contain only letters 
or be blank if no nickname exists.

**********************/

SELECT
    PersonName
    ,IIF(
        PersonName LIKE '%[(]%[A-Za-z]%[)]%'
        ,LTRIM(RTRIM(CONCAT((LTRIM(RTRIM(SUBSTRING(PersonName,1,CHARINDEX('(', PersonName) - 1)))),' ',(LTRIM(RTRIM(SUBSTRING(PersonName,CHARINDEX(')', PersonName) + 1, LEN(PersonName))))))))
        ,IIF(
            PersonName LIKE '%[(]%[^A-Za-z]%[)]%'
            ,REPLACE(
                PersonName
                ,SUBSTRING(PersonName, CHARINDEX('(', PersonName) - 1, LEN(PersonName))
                ,'')
            ,PersonName
            )
    ) as FullName
    ,IIF(
        PersonName like '%[(]%[A-Za-z]%[)]%'
        ,SUBSTRING(PersonName,CHARINDEX('(', PersonName) + 1,CHARINDEX(')', PersonName) - CHARINDEX('(', PersonName) - 1)
        ,null
    ) as Nickname
FROM dbo.Person

/**********************

QUESTION 6

Write a query to return risk data for all patients, all payers 
and a moving average of risk for that patient and payer in dbo.Risk. 

**********************/

SELECT
    r.PersonID
    ,p.PersonName
    ,AttributedPayer
    ,RiskScore
    ,AVG(RiskScore) OVER (PARTITION BY r.PersonId, AttributedPayer ORDER BY RiskDateTime) AS MovingAverage
    ,RiskLevel
    ,RiskDateTime
FROM dbo.Risk r
JOIN dbo.Person p ON p.PersonID = r.PersonID
ORDER BY r.PersonID