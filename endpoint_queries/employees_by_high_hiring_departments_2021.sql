WITH DepartmentHires AS (
    SELECT
        D.ID AS DepartmentID,
        D.DEPARTMENT AS Department,
        COUNT(*) AS HiredEmployeesCount
    FROM
        HIRED_EMPLOYEES H
    JOIN
        DEPARTMENTS D ON H.DEPARTMENT_ID = D.ID
    WHERE
        YEAR(H.DATETIME) = 2021
    GROUP BY
        D.ID, D.DEPARTMENT
)
SELECT
    DH.DepartmentID,
    DH.Department,
    DH.HiredEmployeesCount
FROM
    DepartmentHires DH
JOIN
    (SELECT
        AVG(HiredEmployeesCount) AS AvgHires
     FROM
        DepartmentHires) AvgTable
ON
    DH.HiredEmployeesCount > AvgTable.AvgHires
ORDER BY
    DH.HiredEmployeesCount DESC;
