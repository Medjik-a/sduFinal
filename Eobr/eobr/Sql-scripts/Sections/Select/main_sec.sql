SELECT 
date_add(hour,12,aa.START_DATE) as start_dt,
date_add(hour,12,aa.DEADLINE) as deadline,
date_add(hour,12,aa.FINISH_DATE) as finish_dt,
aac.ID as appeal_id,
aa.REG_NUMBER as reg_number,
multiIf(aa.CURRENT_WORKING_STATE='NEW','Обращение зарегистрировано',
aa.CURRENT_WORKING_STATE='CHECK_63_FIX','На исправлении на соответствие 63 статье АППК',
aa.CURRENT_WORKING_STATE='IN_PROGRESS','На исполнении',
aa.CURRENT_WORKING_STATE LIKE '%ON_AGREEING%','На согласовании',
aa.CURRENT_WORKING_STATE LIKE '%ON_SIGNING%','На подписаниии',
aa.CURRENT_WORKING_STATE='FINISHED','Завершено',
aa.CURRENT_WORKING_STATE='WAIT_HEARING','В ожидании слушания',
aa.CURRENT_WORKING_STATE='WAIT_HEARING','В ожидании слушания',
aa.CURRENT_WORKING_STATE='CHECK_93_FIX','На исправлении на соответствие 93 статье АППК',
'')
as current_working_state,
at2.NAME_RU as appeal_type,
ic.NAME_RU as issue,
is2.NAME_RU as subissue,
adt.NAME_RU as appeal_decision,
o.loc_name as loc_name,
o.loc_type as loc_type,
o.kato as kato,
o.region as region,
o.raion as raion,
o.org_name as org_name,
o.org_type as org_type,
o.can_see_org AS can_see_org,
apc.IINBIN as iin_bin
FROM BTSD_EOBRASHENIYA.APPEALS_APPEALS_APPLICATIONS_ACTUAL aac 
LEFT JOIN BTSD_EOBRASHENIYA.APPEALS_ACTUAL aa ON aac.ID=aa.ID
LEFT JOIN BTSD_EOBRASHENIYA.APPLICANTS_ACTUAL apc ON aac.APPLICANT_ID=apc.ID
LEFT JOIN BTSD_EOBRASHENIYA.APPEALS_DECISION_ACTUAL ada ON aa.ID=ada.ID
LEFT JOIN BTSD_EOBRASHENIYA.APPEAL_DECISION_TYPES adt ON adt.ID=ada.DECISION_TYPE_ID 
LEFT JOIN BTSD_EOBRASHENIYA.APPEAL_TYPES at2 ON at2.ID=aac.TYPE_ID
LEFT JOIN BTSD_EOBRASHENIYA.orglocff o ON o.org_id=aa.EXECUTIVE_ORG_ID
LEFT JOIN BTSD_EOBRASHENIYA.ISSUES_CATEGORIES ic ON ic.ID=aac.ISSUE_CATEGORY_ID
LEFT JOIN BTSD_EOBRASHENIYA.ISSUES_SUBCATEGORIES is2 ON is2.ID=aac.SUBISSUE_CATEGORY_ID