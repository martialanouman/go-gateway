# La voie retour appelle encore ses dépendances sans échéance, hors de la remise au pod

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-304 (revue) · **Portée par :** —

**Ce qu'on a fait à la place.** step-304 borne seulement l'appel au pod (`bindDeliverTimeout` dans
`tryBinds`, `internal/modlrrouter/deliverer.go`). `Deliverer.Deliver` passe toujours le ctx du
consommateur Kafka, sans échéance, à `lookup.Lookup` (gRPC vers `session-manager-svc`), à
`webhooks.Get` (Postgres) et à `producer.Produce` (la dead-letter). Seul le webhook est borné, par
`webhookHotPathTimeout` côté client HTTP.

**Pourquoi.** Le sujet de step-304 était l'attribution des échecs de bind et l'écriture de l'adresse.
Borner les autres appels demande de choisir une échéance par dépendance, et ce qu'on fait de chacune.

**Ce qu'il en coûte.** Un `session-manager-svc` ou un Postgres qui accepte la connexion sans répondre
gèle la partition MO/DLR entière, sans borne, comme le faisait un pod muet avant step-304.

**À quoi on reconnaîtra qu'il faut la payer.** Du lag sur les groupes MO/DLR alors que les pods SMPP sont
sains et les tentatives de remise absentes des journaux.
