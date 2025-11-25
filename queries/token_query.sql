-- Part 1: Get tokens for a specific transcript
SELECT
    t2.id as token_id,
    t2.transcript_id_id,
    t2."ID_Inf_id",
    e.start_time,
    e.end_time,
    t2.token_reihung,
    t2.ortho,
    t2.phon,
    t2.text_in_ortho,
    t2.sppos,
    t2.sptag,
    t2.splemma,
    t2.spdep,
    t2.spenttype
FROM "token" t2
JOIN event e ON e.id = t2.event_id_id
WHERE t2.transcript_id_id = %s -- Placeholder for prepared statement
ORDER BY e.start_time, e.end_time;
