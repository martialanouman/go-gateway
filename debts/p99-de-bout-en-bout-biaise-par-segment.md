# Le p99 de bout en bout est par `submit_sm`, pas par message

> **Statut :** OUVERTE (acceptée) · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:478`) · **Portée par :** —

Un message de N segments produit N observations. Dédupliquer « demanderait un état inter-records par
`message_id`, non borné et concurrent entre shards » — la raison est bonne, le biais reste.

**Ce qu'il en coûte.** Écrit : « compter les précédents biaise la distribution du bon côté
(**optimiste**) ». Le verdict NFR de step-280 sera donc rendu sur une distribution flatteuse, et
**ce n'est écrit nulle part dans `tasks-todo/step-280.md`**.

**À quoi on reconnaîtra qu'il faut la payer.** À la rédaction du verdict NFR : soit on corrige, soit
on écrit le biais à côté du chiffre. La seconde option coûte une phrase.

Source : `tasks-done/step-201.md:478`
