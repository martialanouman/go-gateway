# Deux délais bornent des chemins chauds sans levier de configuration

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-260e et step-270c · **Portée par :** —

`InboundSubmitTimeout` (15 s) et `DefaultLookupTimeout` (2 s) sont des constantes compilées. Les deux
fiches l'ont noté sans le payer : « fiche à part si on veut l'exposer » et « n'a de sens que face à
une vraie file pgx ».

step-390b en a ajouté un troisième : `cdrLookupTimeout` (5 s), qui borne la lecture ClickHouse de
`query_sm` et de `cancel_sm` sur la goroutine de lecture de la session.

**Ce qu'il en coûte.** Non écrite. En pratique : step-280 devra régler ces deux chemins sur un
environnement représentatif, et ne pourra le faire qu'en recompilant.

**À quoi on reconnaîtra qu'il faut la payer.** La première campagne de charge qui veut faire varier
l'un des deux. C'est-à-dire step-280.

Sources : `internal/smpp/session/session.go:38` · `internal/routing/exact/resolver.go:26` ·
`internal/smppserver/smppserver.go` (`cdrLookupTimeout`)
