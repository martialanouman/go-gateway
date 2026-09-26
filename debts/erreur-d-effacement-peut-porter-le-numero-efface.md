# Une erreur d'effacement MSISDN peut porter le numéro effacé jusque dans le log

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-297 · **Portée par :** —

**Ce qu'on a fait à la place.** step-297 a retiré le sujet du log de dernier recours d'une attestation non
enregistrée. Il n'a pas touché au texte d'**erreur** d'un effacement échoué. Ce texte est journalisé
(`internal/adminapi/gdpr.go:174`) et devient l'attestation `erasure failed: <err>`. Or il vient du pilote
ClickHouse, et la mutation inscrit le numéro en littéral dans son prédicat
(`internal/storage/clickhouse/erase.go:68`). Le refus d'un numéro malformé le cite aussi
(`erase.go:63`), bien que la normalisation en amont rende ce chemin peu atteignable.

**Pourquoi.** Hors du périmètre de la fiche, qui portait sur le log d'une attestation non enregistrée
(ADR-0018, Consequences).

**Ce qu'il en coûte si on ne la paie jamais.** Si le serveur ClickHouse renvoie l'instruction fautive dans son
exception, le numéro d'une personne dont l'effacement a échoué survit dans la collecte de logs.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier effacement échoué dont le log montre un numéro, ou
dès qu'un audit demande que les logs soient prouvés exempts de MSISDN.
