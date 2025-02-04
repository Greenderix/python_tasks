
# SQL Задания

## Задание 1: Запрос с использованием SELECT

**Задание:**

У вас есть таблица employees с следующими полями: id, name, salary, department_id. Напишите SQL-запрос, который возвращает имена сотрудников и их зарплаты для всех сотрудников с зарплатой выше средней по всей таблице.
```sql
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);
```

---

## Задание 2: Оконные функции и подзапросы

**Задание:**

В той же таблице employees, напишите запрос, который возвращает имена сотрудников, их зарплаты и ранк по зарплате в пределах их department_id (т.е. номер, указывающий их место в наборе по убыванию зарплаты).
```sql
SELECT
    name,
    salary,
    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank
FROM
    employees;
```

---

## Задание 3: Создание функции

**Задание:**

Создайте функцию get_department_salary_summary(dept_id INT), которая принимает ID отдела и возвращает плоскую таблицу (набор) с общим количеством сотрудников и средней зарплаты в этом отделе.
### Создание функции

```sql
CREATE OR REPLACE FUNCTION get_department_salary_summary(dept_id INT)
RETURNS TABLE (employee_count INT, average_salary NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::INT AS employee_count,
        AVG(salary)::NUMERIC AS average_salary
    FROM
        employees
    WHERE
        department_id = dept_id;
END;
$$ LANGUAGE plpgsql;
```

### Вызов функции

Вместо `DEPT_ID` введите свой параметр в виде целого числа.

```sql
SELECT * FROM get_department_salary_summary(DEPT_ID);
```


