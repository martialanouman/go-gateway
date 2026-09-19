# `AntispamEvaluator.Evaluate` ne retourne jamais d'erreur : la signature ment

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** le code lui-même · **Portée par :** —

Les contrôles adossés à Redis échouent **ouvert** (§1.5) : une panne du magasin marque le message au
lieu de le bloquer. Conséquence assumée, écrite dans le code : « the error return is currently always
nil (**retained for interface stability**) ».

**Ce qu'il en coûte.** Tout appelant qui écrit `if err != nil` sur cette interface écrit du code mort,
et un futur implémenteur qui commencerait à retourner une erreur casserait **en silence** une
politique §1.5 que rien dans le type n'exprime. Le fail-open est une décision ; la signature
conservée pour rien est la dette.

**À quoi on reconnaîtra qu'il faut la payer.** La première implémentation alternative de cette
interface — ou la première revue qui demande pourquoi une erreur toujours nulle traverse le pipeline.

Source : `internal/pipeline/pipeline.go:76`
