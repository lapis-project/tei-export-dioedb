select t2.id from tokentoset t 
	join tokenset t2 on t2.id = t.id_tokenset_id 
	where t.id_token_id = ANY(%s)