CREATE OR REPLACE VIEW workspace.default.employee_metrics__ENV__
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: workspace.default.silver_curated_events__ENV__
  comment: Employee metrics for HR analytics and reporting
  
  dimensions:
    - name: dept_id
      expr: dept_id
      display_name: Department ID
      comment: Employee department identifier
    
    - name: joining_month
      expr: DATE_TRUNC('MONTH', joining_date)
      display_name: Joining Month
      comment: Month when employee joined
    
    - name: age_bracket
      expr: |-
        CASE
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), dob) / 12) < 30 THEN 'Under 30'
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), dob) / 12) < 40 THEN '30-39'
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), dob) / 12) < 50 THEN '40-49'
          ELSE '50+'
        END
      display_name: Age Bracket
    
    - name: tenure_bracket
      expr: |-
        CASE
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), joining_date) / 12) < 1 THEN 'Less than 1 year'
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), joining_date) / 12) < 3 THEN '1-3 years'
          WHEN FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), joining_date) / 12) < 5 THEN '3-5 years'
          ELSE '5+ years'
        END
      display_name: Tenure Bracket
  
  measures:
    - name: employee_count
      expr: COUNT(1)
      display_name: Employee Count
      comment: Total number of employees
      format:
        type: number
        decimal_places:
          type: exact
          places: 0
    
    - name: total_salary
      expr: SUM(salary)
      display_name: Total Salary
      comment: Sum of all employee salaries
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
    
    - name: avg_salary
      expr: AVG(salary)
      display_name: Average Salary
      comment: Average employee salary
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
    
    - name: avg_tenure_years
      expr: AVG(FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), joining_date) / 12))
      display_name: Average Tenure (Years)
      format:
        type: number
        decimal_places:
          type: exact
          places: 1
$$