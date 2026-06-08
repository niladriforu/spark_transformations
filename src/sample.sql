-- Replace dev with your target environment (dev or prod)
select 
    dept_id,
    measure(total_salary) as total_salary
from 
    workspace.default.employee_metrics_dev
group by dept_id;
