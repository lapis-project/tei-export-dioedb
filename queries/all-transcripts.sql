select t.id, 
t.name, 
kdte."Bezeichnung_Erhebung" as "erhebung", 
kdte2."Bezeichnung" as "erhebungsart", 
odto.ort_namekurz, 
odto.ort_namelang, 
odto.lat, 
odto.lon 
from transcript t
join "KorpusDB_tbl_inferhebung" kdti on kdti."id_Transcript_id" = t.id
join "KorpusDB_tbl_erhebungen" kdte on kdte.id = kdti."ID_Erh_id" 
join "KorpusDB_tbl_erhebungsarten" kdte2 on kdte2.id = kdte."Art_Erhebung_id" 
left join "OrteDB_tbl_orte" odto on odto.id = kdti."Ort_id" 
order by t.id;