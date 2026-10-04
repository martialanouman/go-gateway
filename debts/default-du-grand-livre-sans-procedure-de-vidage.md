# Un DEFAULT du grand livre qui a reçu des lignes n'a pas de procédure pour être vidé

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-408 (`docs/guide-ingenierie-passerelle-sms.md:389`) · **Portée par :** —

**Ce qu'on a fait à la place.** step-408 laisse `billing_ledger_default` en filet : une écriture dont le jour n'a
pas de partition y tombe sans échouer. L'alerte `max(billing_ledger_default_rows) > 0` le signale, mais rien ne
sort ces lignes de `DEFAULT`, et aucun runbook ne dit comment.

**Pourquoi.** Sur le VPS de test, le vidage est un `TRUNCATE` cohérent (fiche de step-408), parce qu'on peut y
perdre la facturation. En production, on ne peut pas : il faudrait déplacer les lignes du jour dans une table
neuve puis l'attacher, sous verrou, sans casser « somme du grand livre = solde ». C'est une procédure à écrire
et à tester, que la step n'avait pas à livrer pour que le cas nominal fonctionne.

**Ce qu'il en coûte.** Tant que `DEFAULT` garde des lignes d'un jour, la partition de ce jour ne s'attache
jamais : tout son trafic y tombe, et l'ATTACH échoue à chaque passe horaire, en 200 ms au plus, avec un Warn.
Chaque création de partition parcourt `DEFAULT` : passé 200 ms de parcours, plus aucune partition ne se crée,
et les jours suivants tombent à leur tour dans `DEFAULT`.

**À payer** dès la première alerte, ou avant le go-live si l'on veut un runbook prêt (guide §13.3).
