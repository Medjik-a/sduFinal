INSERT INTO MCRIAP_EOBR.segmentation_appeals
(appeal_id, reg_number, appeal_decision, iin, family_id, family_cat, `sa.ID_SK_FAMILY_QUALITY2`, `sa.FULL_KATO_NAME`, `sa.KATO_2_NAME`, `sa.KATO_2`, `sa.KATO_4_NAME`, `sa.KATO_4`, `sa.FAMILY_CAT`, `sa.filtr1`, `sa.filtr2`, `sa.filtr3`, `sa.filtr4`, `sa.filtr5`, `sa.filtr6`, `sa.filtr7`, `sa.filtr8`, `sa.filtr9`, `sa.filtr10`, `sa.sg3.ID_SK_FAMILY_QUALITY2`, `sa.filtr11`, `sa.filtr12`, `sa.filtr13`, `sa.filtr14`, `sa.filtr15`, `sa.filtr16`, `sa.filtr17`, `sa.filtr18`, `sa.filtr19`, `sa.filtr20`, `sa.filtr21`, `sa.filtr22`, `sa.filtr23`, `sa.count_iin`)
SELECT
ms.appeal_id as appeal_id,
ms.reg_number as reg_number,
ms.appeal_decision as appeal_decision,
fii.IIN as iin,
fii.FAMILY_ID as family_id,
sa.FAMILY_CAT as family_cat,
sa.*
FROM MCRIAP_EOBR.main_sec_tmp ms
LEFT JOIN MCRIAP_EOBR.family_id_iin fii ON fii.IIN=ms.iin_bin
LEFT JOIN MCRIAP_EOBR.SEGMENTATION_ASSOGIN sa ON sa.ID_SK_FAMILY_QUALITY2=toString(fii.FAMILY_ID) 
WHERE iin<>''