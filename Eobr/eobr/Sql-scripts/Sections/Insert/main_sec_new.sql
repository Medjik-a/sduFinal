INSERT INTO MCRIAP_EOBR.main_sec
(start_dt, deadline, finish_dt, appeal_id, reg_number, current_working_state,appeal_type, issue, subissue, appeal_decision, loc_name, loc_type, kato, region, raion, org_name, org_type, can_see_org, iin_bin, family_id, family_cat)
SELECT 
ms.start_dt as start_dt,
ms.deadline as deadline,
ms.finish_dt as finish_dt,
ms.appeal_id as appeal_id,
ms.reg_number as reg_number,
ms.current_working_state as current_working_state,
ms.appeal_type as appeal_type,
ms.issue as issue,
ms.subissue as subissue,
ms.appeal_decision as appeal_decision,
ms.loc_name as loc_name,
ms.loc_type as loc_type,
ms.kato as kato,
ms.region as region,
ms.raion as raion,
ms.org_name as org_name,
ms.org_type as org_type,
ms.can_see_org AS can_see_org,
ms.iin_bin as iin_bin,
sa.ID_SK_FAMILY_QUALITY2 AS family_id,
sa.FAMILY_CAT as family_cat
FROM MCRIAP_EOBR.main_sec_tmp ms
LEFT JOIN MCRIAP_EOBR.family_id_iin fii ON fii.IIN=ms.iin_bin
LEFT JOIN MCRIAP_EOBR.SEGMENTATION_ASSOGIN sa ON sa.ID_SK_FAMILY_QUALITY2=toString(fii.FAMILY_ID) 