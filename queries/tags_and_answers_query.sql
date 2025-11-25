-- Part 2: Get answers and tags for a list of token IDs
WITH child_tags_agg AS (
    -- Pre-calculate children for ALL parents once
    SELECT
        tafam."id_ParentTag_id",
        json_agg(
            json_build_object(
                'childtags', tafam."id_ChildTag_id",
                'taglang', child_kdtt."Tag_lang",
                'tag_id', child_kdtt.id,
                'tag_name', child_kdtt."Tag"
            )
        ) as children_json
    FROM "KorpusDB_tbl_tagfamilie" tafam
    JOIN "KorpusDB_tbl_tags" child_kdtt ON child_kdtt.id = tafam."id_ChildTag_id"
    GROUP BY tafam."id_ParentTag_id"
),
tags_per_ebene AS (
    SELECT
        kdta2."id_Antwort_id",
        kdta2."id_TagEbene_id",
        json_agg(
            json_build_object(
                'Tag', kdtt."Tag",
                'Tag_lang', kdtt."Tag_lang",
                'Tag_id', kdtt."id",
                'Kommentar', kdtt."Kommentar",
                'Reihung', kdta2."Reihung",
                -- Join the pre-calculated JSON here.
                -- COALESCE ensures we return an empty array [] instead of NULL if no children exist.
                'children_ids', COALESCE(cta.children_json, '[]'::json)
            ) ORDER BY kdta2."Reihung"
        ) as tags
    FROM
        "KorpusDB_tbl_antwortentags" kdta2
    JOIN "KorpusDB_tbl_tags" kdtt ON kdtt.id = kdta2."id_Tag_id"
    -- The optimization happens here: a single JOIN instead of a subquery
    LEFT JOIN child_tags_agg cta ON cta."id_ParentTag_id" = kdtt.id
    GROUP BY
        kdta2."id_Antwort_id",
        kdta2."id_TagEbene_id"
),
tags_grouped_by_ebene AS (
    SELECT
        tpe."id_Antwort_id",
        json_object_agg(tpe."id_TagEbene_id", tpe.tags) as tags
    FROM tags_per_ebene tpe
    WHERE tpe."id_TagEbene_id" IS NOT NULL
    GROUP BY tpe."id_Antwort_id"
),
antworten_agg AS (
    SELECT
        kdta.ist_token_id,
        json_agg(
            json_build_object(
                'id', kdta.id,
                'text', kdta."Kommentar",
                'tags', ta.tags
            )
        ) AS antworten
    FROM
        "KorpusDB_tbl_antworten" kdta
    LEFT JOIN tags_grouped_by_ebene ta ON ta."id_Antwort_id" = kdta.id
    WHERE kdta.ist_token_id = ANY(%s) -- Placeholder for list of token IDs
    GROUP BY
        kdta.ist_token_id
)
SELECT
    aa.ist_token_id,
    aa.antworten
FROM
    antworten_agg aa;
