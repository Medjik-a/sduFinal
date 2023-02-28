INSERT INTO SDM_AUTO_REPORTS.ME_PSER_VITRINA
(id_indicator, name_indicator, id_period, Name_period, y2021, y2022, id_year, Name_year, id_date, Name_date, id_unit, Name_unit, id_plan, p_value, f_value)
SELECT DISTINCT set1.id_indicator as id_indicator,
          set1.name_indicator as name_indicator,
          set1.id_period as id_period,
          set2.Name_period as Name_period,
          (case set3.Name_year
               when 2021 then 2021
               else 0
           end) as y2021,
          (case set3.Name_year
               when 2022 then 2022
               else 0
           end) as y2022,
          set1.id_year as id_year,
          set3.Name_year as Name_year,
          set1.id_date as id_date,
          set4.Name_date as Name_date,
          set1.id_unit as id_unit,
          set5.Name_unit as Name_unit,
         set1.id_plan as id_plan,
          set6.p_value as p_value,
         set1.f_value as f_value
   FROM (SELECT * FROM SDM_AUTO_REPORTS.ME_Fact WHERE id_period=1) as set1
    LEFT JOIN
    (SELECT id_plan,
             id_period,
             id_indicator,
             p_value,
             id_unit,
             id_year,
             id_date,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_Plan) as set6 on set1.id_plan = set6.id_plan AND set1.id_year=set6.id_year
     LEFT JOIN
     (SELECT id_period,
             Name_period
      FROM SDM_AUTO_REPORTS.ME_D_Type_Period) as set2 on set1.id_period = set2.id_period
    LEFT join
     (SELECT id_year,
             Name_year,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Year) as set3 on set1.id_year = set3.id_year
    LEFT join
     (SELECT id_date,
             Name_date,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Period) as set4 on set1.id_date = set4.id_date
    LEFT join
     (SELECT id_unit,
             Name_unit,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Measure) as set5 on set1.id_unit = set5.id_unit
UNION ALL
SELECT DISTINCT set1.id_indicator as id_indicator,
          set1.name_indicator as name_indicator,
          set1.id_period as id_period,
          set2.Name_period as Name_period,
          (case set3.Name_year
               when 2021 then 2021
               else 0
           end) as y2021,
          (case set3.Name_year
               when 2022 then 2022
               else 0
           end) as y2022,
          set1.id_year as id_year,
          set3.Name_year as Name_year,
          set1.id_date as id_date,
          set4.Name_date as Name_date,
          set1.id_unit as id_unit,
          set5.Name_unit as Name_unit,
         set1.id_plan as id_plan,
          set6.p_value as p_value,
         set1.f_value as f_value
   FROM (SELECT * FROM SDM_AUTO_REPORTS.ME_Fact WHERE id_period=2)as set1
    LEFT JOIN
    (SELECT id_plan,
             id_period,
             id_indicator,
             p_value,
             id_unit,
             id_year,
             id_date,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_Plan ) as set6 on set1.id_plan = set6.id_plan AND set6.id_year=set1.id_year AND set1.id_date=set6.id_date
     LEFT JOIN
     (SELECT id_period,
             Name_period
      FROM SDM_AUTO_REPORTS.ME_D_Type_Period) as set2 on set1.id_period = set2.id_period
    LEFT join
     (SELECT id_year,
             Name_year,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Year) as set3 on set1.id_year = set3.id_year
    LEFT join
     (SELECT id_date,
             Name_date,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Period) as set4 on set1.id_date = set4.id_date
    LEFT join
     (SELECT id_unit,
             Name_unit,
             SDU_LOAD_IN_DT
      FROM SDM_AUTO_REPORTS.ME_D_Measure) as set5 on set1.id_unit = set5.id_unit