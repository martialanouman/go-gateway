# N+1 sur `list-routes` et `get-route`

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** revue M1 (`docs/plan-execution-passerelle.md:254`) · **Portée par :** —

Les cibles sont lues route par route, hors transaction : « Known limitation (M1): this reads targets
per route (1 + N round-trips) ». Raison écrite : « **acceptable au débit du plan de contrôle** ; à
batcher (`WHERE route_id = ANY($1)`) quand le volume l'exigera ». Le correctif est même écrit dans le
commentaire.

**Ce qu'il en coûte.** Non écrite, et c'est le problème : le seuil (« quand le volume l'exigera »)
n'est instrumenté nulle part. Depuis step-260c on sait que l'auth REST et le bind SMPP n'ont **aucun
cache** et partagent la base de l'Admin — un `list-routes` lourd concurrence donc directement le
chemin chaud.

**À quoi on reconnaîtra qu'il faut la payer.** Une latence de bind ou d'auth qui bouge quand un
opérateur ouvre la page des routes.

Source : `internal/storage/postgres/routes.go:78`
