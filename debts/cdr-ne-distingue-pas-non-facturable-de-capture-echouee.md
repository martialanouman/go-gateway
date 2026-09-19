# Le CDR ne distingue pas « non facturable » de « capture échouée »

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-190 (`tasks-done/step-190.md:87`) · **Portée par :** —

step-190 a livré le reaper et s'est arrêtée là : la colonne `billable` sur le CDR est renvoyée à un
« suivi distinct » que personne n'a ouvert. Le godoc l'écrit sans détour — `Billed` vaut `false`
« when nothing was captured (billing disabled, no reservation, **or a fail-open capture**) ».

**Ce qu'il en coûte.** Non écrite dans la fiche. En pratique, une ligne `billed=0,
credits_charged=NULL` est ambiguë : un litige client ou une réconciliation ne peut pas dire, depuis le
CDR seul, si le message n'était pas facturable ou si la capture a échoué. step-201c répond qu'« un
litige s'arbitre sur le grand livre » — vrai, mais cela suppose d'ouvrir le grand livre pour chaque
ligne suspecte.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier litige de facturation qu'on n'arbitre pas
en lisant le CDR.

Sources : `internal/pipeline/envelope.go:135` · `migrations/clickhouse/0001_cdr.up.sql:46`
