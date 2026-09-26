# Un test du budget de drain suppose qu'une goroutine est ordonnancée en 1 ms

> **Statut :** OUVERTE · **Nature :** test
> **Née de :** step-397 (constatée dans `make check`) · **Portée par :** —

`TestABudgetThatExpiresOnAFinishedComponentReportsNothing/group` a échoué une fois dans `make check`
(`drain budget exceeded after 1ns: already-finished still running`), puis a passé 400 fois de suite
une fois lancé seul. Le test suppose que le composant, qui rend `nil` aussitôt, a terminé pendant le
`time.Sleep(time.Millisecond)` qui précède `cancel()`. Sous la charge de la suite complète, la
goroutine n'a pas encore tourné, et le budget de 1 ns rapporte un dépassement réel : le code a raison,
c'est la prémisse du test qui est fausse.

**Ce qu'il en coûte.** Un rouge intermittent de `make check` et de la CI, sans rapport avec le diff en
cours.

**À quoi on reconnaîtra qu'il faut la payer.** Le prochain rouge de ce test en CI. Le remède tient
dans le test : attendre que le composant ait rendu la main (un canal fermé par lui) avant `cancel()`,
au lieu de dormir.

Sources : `internal/platform/supervisor/supervisor_test.go:472-479` · `internal/platform/supervisor/supervisor.go:142`
