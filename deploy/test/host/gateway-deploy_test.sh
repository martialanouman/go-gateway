#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
cat >"$tmp/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$KUBECTL_LOG"
echo "$KUBECONFIG" >>"$KUBECONFIG_LOG"
case "$*" in
  *"get statefulset,deployment"*) echo statefulset.apps/postgres ;;
  *"get deployment"*) echo deployment.apps/router-svc ;;
  *"get job -l gateway.test/phase=deps-job"*) echo rustfs-bucket ;;
  *"get job -l gateway.test/phase=job"*) echo migrate ;;
  *"get job rustfs-bucket -o jsonpath="*|*"get job migrate -o jsonpath="*)
    if [[ -n ${FAIL_JOB:-} ]]; then echo "Failed=True,"; else echo "Complete=True,"; fi ;;
  *"get job -l gateway.test/phase=seed"*) echo test-seed ;;
  *"get job -l gateway.test/phase=smoke"*) echo smoke ;;
  *"get job test-seed -o jsonpath="*) echo "Complete=True," ;;
  *"get job smoke -o jsonpath="*)
    if [[ -n ${FAIL_SMOKE:-} ]]; then echo "Failed=True,"; else echo "Complete=True,"; fi ;;
  # Le pod réussi du déploiement précédent peut survivre à la suppression de son Job : seul le plus
  # récent porte l'id de ce seed.
  *"get pods -l job-name=test-seed --field-selector=status.phase=Succeeded"*)
    if [[ $* == *"--sort-by=.metadata.creationTimestamp"* && $* == *"{.items[-1:].metadata.name}"* ]]; then
      echo test-seed-new
    else
      echo test-seed-old
    fi ;;
  *"logs test-seed-new"*)
    [[ -n ${NO_SEED_LINE:-} ]] || echo "connector_id=${SEED_ID:-11111111-1111-1111-1111-111111111111}" ;;
  *"logs test-seed-old"*) echo "connector_id=22222222-2222-2222-2222-222222222222" ;;
  # Adresse heuristique (« un pod de ce Job », sans filtre Succeeded) : elle peut tomber sur un pod
  # échoué d'une tentative précédente. Toujours muette ici, pour prouver que gateway-deploy ne s'y fie
  # plus.
  *"logs job/test-seed"*) : ;;
  *"get configmap test-seed"*) echo "${CURRENT_ID:-}" ;;
esac
exit 0
EOF
chmod +x "$tmp/bin/kubectl"
fail() { echo "gateway-deploy_test: $*" >&2; exit 1; }

# Étiquette une ligne de log kubectl par phase. La lecture individuelle d'un Job (ex. "get job
# migrate") ne mentionne pas la phase : on la rattache par nom, connu de ce fixture.
label_of() {
  case $1 in
    *"phase=deps-job"*|*"job rustfs-bucket"*) echo deps-job ;;
    *"phase=deps"*) echo deps-apply ;;
    *"phase=job"*|*"job migrate"*) echo job ;;
    *"!gateway.test/phase"*) echo app-apply ;;
    *"rollout status deployment.apps/router-svc"*) echo app-rollout ;;
    *"phase=seed"*|*"job test-seed"*|*"job/test-seed"*|*"job-name=test-seed"*|*"logs test-seed-"*) echo seed ;;
    *"phase=smoke"*|*"job smoke"*) echo smoke ;;
    *"rollout restart"*) echo restart ;;
    *"rollout status deployment/connector-pool-svc"*) echo rollout ;;
    *"configmap test-seed"*|*"apply -f -"*) echo seed ;;
    *) ;;
  esac
}

# grep -nE trie par numéro de ligne, donc toujours croissant : il ne peut jamais révéler une
# inversion de phases. On compare plutôt la séquence des étiquettes (dédupliquée) au log réel.
sequence_of() {
  local prev="" lbl line seq=""
  while IFS= read -r line; do
    lbl=$(label_of "$line")
    [[ -n $lbl && $lbl != "$prev" ]] && seq+="$lbl "
    prev=$lbl
  done <"$1"
  echo "${seq% }"
}

export KUBECTL_LOG="$tmp/log" KUBECONFIG_LOG="$tmp/kubeconfig-log"
echo 'kind: ConfigMap' | PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null
seq=$(sequence_of "$KUBECTL_LOG")
[[ $seq == "deps-apply deps-job job app-apply app-rollout seed restart rollout smoke" ]] || fail "ordre violé : $seq -- log : $(cat "$KUBECTL_LOG")"
[[ $(head -n1 "$KUBECONFIG_LOG") == */.kube/config ]] || fail "KUBECONFIG n'est pas exporté vers ~/.kube/config"
grep -q 'rollout restart deployment/connector-pool-svc' "$KUBECTL_LOG" || fail "connector-pool-svc n'est pas redémarré"
grep -q 'create configmap test-seed --from-literal=CONNECTOR_ID=11111111-1111-1111-1111-111111111111 ' "$KUBECTL_LOG" \
  || fail "CONNECTOR_ID ne vient pas du pod de seed le plus récent -- log : $(cat "$KUBECTL_LOG")"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | FAIL_JOB=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un Job en échec n'arrête pas le déploiement"
fi
! grep -q '!gateway.test/phase' "$KUBECTL_LOG" || fail "l'application est appliquée malgré un Job en échec"

: >"$KUBECTL_LOG"
echo 'kind: ConfigMap' | CURRENT_ID=11111111-1111-1111-1111-111111111111 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null
seq=$(sequence_of "$KUBECTL_LOG")
[[ $seq == "deps-apply deps-job job app-apply app-rollout seed smoke" ]] || fail "id inchangé mais redémarrage : $seq -- log : $(cat "$KUBECTL_LOG")"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | FAIL_SMOKE=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un smoke test en échec n'arrête pas le déploiement"
fi

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | NO_SEED_LINE=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "l'absence de connector_id dans les logs du seed n'arrête pas le déploiement"
fi
! grep -q 'configmap' "$KUBECTL_LOG" || fail "la configmap est écrite sans connector_id valide"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | SEED_ID=pas-un-uuid PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un connector_id qui n'est pas un UUID n'arrête pas le déploiement"
fi
! grep -q 'configmap' "$KUBECTL_LOG" || fail "la configmap est écrite avec un connector_id invalide"
echo "gateway-deploy_test: ok"
