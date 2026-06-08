select 
    dept_id,
    measure(total_salary) as total_salary
from 
    workspace.default.employee_metrics
group by dept_id;