select
	pdti.id as inf_id,
	pdti.inf_sigle as sigle,
	pdtig.gruppe_bez as age_group,
	pdtp.weiblich as is_female,
	odto.ort_namekurz,
	odto.ort_namelang,
	odto.lat,
	odto.lon,
	odto.osm_id
from
	"PersonenDB_tbl_informanten" pdti
join "PersonenDB_tbl_personen" pdtp on
	pdtp.id = pdti.id_person_id
left join "PersonenDB_tbl_informantinnen_gruppe" pdtig on
	pdtig.id = pdti.inf_gruppe_id
left join "OrteDB_tbl_orte" odto on
	odto.id = pdti.geburtsort_id
where
	pdti.id ANY(%s)