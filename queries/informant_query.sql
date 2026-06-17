select
	pdti.id as inf_id,
	pdti.inf_sigle as sigle,
	pdtig.gruppe_bez as age_group,
	pdtp.weiblich as is_female,
	odto.ort_namekurz,
	odto.ort_namelang,
	odto.lat,
	odto.lon,
	odto.osm_id,
	pdtig.gruppe_team_id,
	pdtt.team_bez,
	nullif(pdti.ausbildung_max, '') as ausbildung_max,
	nullif(pdti.ausbildung_spez , '') as ausbildung_spez,
	pdti.migrationsklasse 
from
	"PersonenDB_tbl_informanten" pdti
left join "OrteDB_tbl_orte" odto on
	odto.id = pdti.inf_ort_id 
join "PersonenDB_tbl_personen" pdtp on
	pdtp.id = pdti.id_person_id
left join "PersonenDB_tbl_informantinnen_gruppe" pdtig on
	pdtig.id = pdti.inf_gruppe_id
left join "PersonenDB_tbl_teams" pdtt on pdtt.id = pdtig.gruppe_team_id where
	pdti.id = ANY(%s)