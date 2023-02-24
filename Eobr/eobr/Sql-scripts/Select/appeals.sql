SELECT
 start_date
 ,deadline
, finish_date
, "org_id" AS "ORG_ID" 
, "signer" AS "SIGNER" 
, created_date
, modified_date
, "hidden" AS "HIDDEN" 
, "checked_by_controller" AS "CHECKED_BY_CONTROLLER" 
, "executive_org_id" AS "EXECUTIVE_ORG_ID" 
, "prolong_count" AS "PROLONG_COUNT" 
, "complaint_exist" AS "COMPLAINT_EXIST" 
, "executor_id" AS "EXECUTOR_ID" 
, "has_been_hearing" AS "HAS_BEEN_HEARING" 
, "id" AS "ID" 
, "reg_number" AS "REG_NUMBER" 
, "current_state" AS "CURRENT_STATE" 
, "current_working_state" AS "CURRENT_WORKING_STATE" 
, "appeal_source_id" AS "APPEAL_SOURCE_ID" 
, "correlation_id" AS "CORRELATION_ID" 
FROM public."appeals"