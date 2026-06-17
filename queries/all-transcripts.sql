select t.id, 
t.name, 
kdte."Bezeichnung_Erhebung" as "erhebung", 
kdte2."Bezeichnung" as "erhebungsart"
from transcript t
join "KorpusDB_tbl_inferhebung" kdti on kdti."id_Transcript_id" = t.id
join "KorpusDB_tbl_erhebungen" kdte on kdte.id = kdti."ID_Erh_id" 
join "KorpusDB_tbl_erhebungsarten" kdte2 on kdte2.id = kdte."Art_Erhebung_id" 
order by t.id;