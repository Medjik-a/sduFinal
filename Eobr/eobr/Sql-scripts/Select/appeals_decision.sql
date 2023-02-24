SELECT
 "decision_type_id" AS "DECISION_TYPE_ID" 
 , "signer_id" AS "SIGNER_ID" 
, "executor_id" AS "EXECUTOR_ID" 
, created_date
, modified_date
, "termination_type_id" AS "TERMINATION_TYPE_ID" 
, "hearing_need" AS "HEARING_NEED" 
, hearing_period_start
, hearing_period_end 
, "id" AS "ID" 
, "hearing_status" AS "HEARING_STATUS" 
, "message" AS "MESSAGE" 
, "files" AS "FILES" 
, "signature" AS "SIGNATURE" 
, "hearing_protocol_note" AS "HEARING_PROTOCOL_NOTE" 
, "hearing_protocol_file" AS "HEARING_PROTOCOL_FILE" 
, "hearing_protocol_comment" AS "HEARING_PROTOCOL_COMMENT" 
, "hearing_protocol_comment_file" AS "HEARING_PROTOCOL_COMMENT_FILE" 
, "content_in" AS "CONTENT_IN" 
, "readiness" AS "READINESS" 
, "applicant_feedback" AS "APPLICANT_FEEDBACK" 
, "hearing_way" AS "HEARING_WAY" 
, "pre_decision_file" AS "PRE_DECISION_FILE" 
, "hearing_not_held_reason" AS "HEARING_NOT_HELD_REASON" 
FROM public."appeals_decision"