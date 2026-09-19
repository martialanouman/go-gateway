# La garde contrat ↔ implémentation ne compare pas les paramètres de requête

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-320 (`tasks-done/step-320.md:288`) · **Portée par :** —

La garde compare les `operationId`, les codes de réponse et les schémas de requête/réponse — pas les
paramètres de requête. Raison écrite, et honnête : « son coût est le bruit qu'elle produira sur 103
opérations, ce qui est précisément pourquoi elle n'appartient pas à cette fiche ».

**Ce qu'il en coûte.** Écrit : « un `?groupId=` déclaré au contrat et non lu par le handler passerait
donc inaperçu ». C'est exactement le scénario de step-330 (groupes) et step-380 (ventilation par
groupe), toutes deux à faire.

**À quoi on reconnaîtra qu'il faut la payer.** À l'implémentation de step-330 ou step-380 : c'est là
que le premier paramètre déclaré-non-lu peut naître sans que rien ne l'attrape.

Source : `internal/adminapi/contract_test.go`
