# `test-billing-provider` deviendra un moyen d'exfiltration, et rien ne le déclenchera

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-147, rappelée par step-296 · **Portée par :** — (step-296 porte le constat, pas le déclencheur)

L'opération est classée **lecture** par `readOnlyRequest`, et ce classement est juste *aujourd'hui* :
le handler ne fait que charger le fournisseur et répondre un OK de façade (`detail := "stub provider:
real HTTP connectivity probe deferred"`).

**Ce qu'il en coûte.** Écrit : le jour où la sonde devient réelle, « l'opération deviendra un **appel
sortant vers un tiers, avec des identifiants stockés**, déclenché sous `admin:write`. Une sonde est un
moyen d'exfiltration commode — elle prouve qu'une URL répond, et le `base_url` est modifiable par la
même API. »

**À quoi on reconnaîtra qu'il faut la payer.** Rien ne le dira, et c'est tout le problème :
« **aucun test ne peut l'imposer, et c'est le point** » — remplacer le stub ne change ni le chemin, ni
l'identifiant, ni la liste, donc `TestReadOnlyPostSuffixesNameKnownDiagnostics` **reste vert**. Le
déclencheur est humain : quiconque implémente la sonde doit relire ce fichier.

Source : `internal/adminapi/billing_admin.go:393`
