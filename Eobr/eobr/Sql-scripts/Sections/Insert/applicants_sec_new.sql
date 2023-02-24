INSERT INTO MCRIAP_EOBR.applicants_sec(start_dt, appeal_id, reg_number, hidden, appeal_type, issue, subissue, iin_bin, applicant_type, age, citizenship, gender, nationality, social_status, occupation, company_type, family_id, family_cat) 
SELECT 
ast.start_dt as start_dt,
ast.appeal_id as appeal_id,
ast.reg_number as reg_number,
ast.hidden as hidden,
ast.appeal_type as appeal_type,
ast.issue as issue,
ast.subissue as subissue,
ast.iin_bin as iin_bin,
ast.applicant_type as applicant_type,
ast.age as age,
ast.citizenship as citizenship,
ast.gender as gender,
ast.nationality as nationality,
ast.social_status as social_status,
ast.occupation as occupation,
ast.company_type as company_type,
sa.ID_SK_FAMILY_QUALITY2 as family_id,
sa.FAMILY_CAT as family_cat
FROM MCRIAP_EOBR.applicants_sec_tmp as ast
LEFT JOIN MCRIAP_EOBR.family_id_iin fii ON fii.IIN=ast.iin_bin
LEFT JOIN MCRIAP_EOBR.SEGMENTATION_ASSOGIN sa ON sa.ID_SK_FAMILY_QUALITY2=toString(fii.FAMILY_ID) 