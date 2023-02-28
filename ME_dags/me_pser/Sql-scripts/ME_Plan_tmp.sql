INSERT INTO SDM_AUTO_REPORTS.ME_Plan
(id_plan, id_period, id_indicator, p_value, id_unit, id_year, id_date)
SELECT DISTINCT id_plan, id_period, id_indicator, p_value, id_unit, id_year, id_date
FROM SDM_AUTO_REPORTS.ME_Plan_tmp