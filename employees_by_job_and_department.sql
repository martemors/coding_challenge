WITH CTE AS (
    SELECT
        D.DEPARTMENT AS Department,
        J.JOB AS Job,
        DATEPART(QUARTER, H.DATETIME) AS Quarter,
        COUNT(*) AS HiredEmployeesCount
    FROM
        HIRED_EMPLOYEES H
    JOIN
        DEPARTMENTS D ON H.DEPARTMENT_ID = D.ID
    JOIN
        JOBS J ON H.JOB_ID = J.ID
    WHERE
        YEAR(H.DATETIME) = 2021
)
SELECT * FROM CTE
ORDER BY Department, Job, Quarter;