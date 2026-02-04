select kdta.ist_tokenset_id,
    kdta2."Reihung" as "tag_reihung",
    kdtt."Tag" as "tag_name",
    kdtt.id as "tag_id",
    kdtt."Tag" as "tag",
    kdtt."Generation" as "tag_gene",
    t3.id as "token_id"
from
    "KorpusDB_tbl_antworten" kdta
join "KorpusDB_tbl_antwortentags" kdta2 on
    kdta2."id_Antwort_id" = kdta.id
join "KorpusDB_tbl_tags" kdtt on
    kdtt.id = kdta2."id_Tag_id"
join "KorpusDB_tbl_tagebene" kdtt2 on
    kdtt2.id = kdta2."id_TagEbene_id"
join tokenset t on t.id = kdta.ist_tokenset_id 
join tokentoset t2 on t2.id_tokenset_id = t.id 
join "token" t3 on t3.id = t2.id_token_id 
where
	kdta.ist_tokenset_id = ANY(%s)
order by 
    t3.id,
    kdta2."Reihung" 