SELECT
 "type_id" AS "TYPE_ID" 
 , "issue_category_id" AS "ISSUE_CATEGORY_ID" 
, "subissue_category_id" AS "SUBISSUE_CATEGORY_ID" 
, "org_id" AS "ORG_ID" 
, "assistant_user_id" AS "ASSISTANT_USER_ID" 
, created_date AS "CREATED_DATE" 
, modified_date AS "MODIFIED_DATE" 
, "language_id" AS "LANGUAGE_ID" 
, "applicant_id" AS "APPLICANT_ID" 
, "appeal_character" AS "APPEAL_CHARACTER" 
, "id" AS "ID" 
, "location_id" AS "LOCATION_ID" 
, "response_get_way" AS "RESPONSE_GET_WAY" 
, "appeal_form" AS "APPEAL_FORM"
,complaint_appeal_id
,complaint_organization_id
,complaint_location_id
,complaint_executive_user_id
,complaint_type_id
FROM public."av_appeals_applications"