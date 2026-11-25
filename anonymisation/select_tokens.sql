SELECT
    t.id,
    t.transcript_id_id,
    t."ID_Inf_id",
    t.ortho,
    t.text,
    t.text_in_ortho
FROM public.token t
LEFT JOIN public.event e ON e.id = t.event_id_id
ORDER BY
    t.transcript_id_id NULLS LAST,
    t."ID_Inf_id" NULLS LAST,
    e.start_time NULLS LAST,
    e.end_time NULLS LAST,
    t.token_reihung NULLS LAST,
    t.id;
