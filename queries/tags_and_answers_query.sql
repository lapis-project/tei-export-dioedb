select kdta.ist_token_id,
    ARRAY_AGG(kdta2."Reihung" ORDER BY kdta2."Reihung") as "tag_reihung",
    kdtt."Tag" as "tag_name",
    ARRAY_AGG(kdtt.id ORDER BY kdta2."Reihung") as "tag_id",
    ARRAY_AGG(kdtt."Tag" ORDER BY kdta2."Reihung") as "tag",
    ARRAY_AGG(kdtt."Generation" ORDER BY kdta2."Reihung") as "tag_gene"
from
    "KorpusDB_tbl_antworten" kdta
join "KorpusDB_tbl_antwortentags" kdta2 on
    kdta2."id_Antwort_id" = kdta.id
join "KorpusDB_tbl_tags" kdtt on
    kdtt.id = kdta2."id_Tag_id"
join "KorpusDB_tbl_tagebene" kdtt2 on
    kdtt2.id = kdta2."id_TagEbene_id"
where
    kdta.ist_token_id = ANY(%s)
group by
    kdta.ist_token_id,
    kdtt."Tag";