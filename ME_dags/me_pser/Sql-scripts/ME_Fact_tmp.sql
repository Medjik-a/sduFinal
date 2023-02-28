INSERT INTO SDM_AUTO_REPORTS.ME_Fact
(id_indicator, name_indicator, id_period, id_year, id_date, id_unit, id_plan, f_value)
SELECT DISTINCT id_indicator, name_indicator, id_period, id_year, id_date, id_unit, id_plan, f_value
FROM SDM_AUTO_REPORTS.ME_Fact_tmp