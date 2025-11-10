# Transfer der Transkripte nach TEI

Arbeitsschritte die notwendig sind um die Daten zu bekommen:
Die Verbindung zwischen token und Antworten kann auf 2 verschiedenen Arten entstehen bzw ist auf diese Arten modelliert:
- token -> antworten über is_token_id
- als tokenset über tokentoset bzw tokentoset_cache -> tokenset -> antworten über ist_tokenset_id

Für die einfache Variante muss nur die antworten Tabelle über einen LEFT JOIN mit der token tabelle verbunden werden

Bei der Variante mit den Tokensets ist das schwieriger. Wichtig dabei ist es zu wissen dass die Verbindung dadurch definiert ist dass in antworten ist_tokenset_id gesetzt ist. 
Danach relation auf tokenset (von_token, bis_token sind (wahrscheinlich) nur da um die IDs der Anfang und Endtoken einfach zu haben -> Sets sind nicht nach ID geordnet). 
Wenn von_token & bis_token NULL sind -> auf **tokentoset** -> enthält Verbindung auf token im tokenset
Sonst -> auf **tokentoset_cache** -> Verbindung auf token tabelle
