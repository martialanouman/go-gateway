# `metrics.stream` n'a pas de registre de schéma : un entier tient le contrat

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** le code lui-même · **Portée par :** —

L'aveu est dans le champ : « the topic has no schema registry, so this field is the **only thing
standing between a format change and a silently misreading consumer** ».

**Ce qu'il en coûte.** Un producteur qui oublie de bumper `v` ne casse rien de visible — le tableau
de bord affiche des chiffres faux. Le flux est best-effort **par conception**, et c'est assumé ; ce
qui ne l'est pas, c'est qu'un tiers consommant le topic n'a aucun filet.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier changement de format de ce topic, ou le
premier consommateur hors de ce dépôt.

Source : `internal/metricstream/metricstream.go:19`
