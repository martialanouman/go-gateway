#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
cat >"$tmp/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$KUBECTL_LOG"
case "$*" in
  *"get statefulset,deployment"*) echo statefulset.apps/postgres ;;
  *"get deployment"*) echo deployment.apps/router-svc ;;
  *"get job -l gateway.test/phase=deps-job"*) echo rustfs-bucket ;;
  *"get job -l gateway.test/phase=job"*) echo migrate ;;
  *"get job rustfs-bucket -o jsonpath="*|*"get job migrate -o jsonpath="*)
    if [[ -n ${FAIL_JOB:-} ]]; then echo "Failed=True,"; else echo "Complete=True,"; fi ;;
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

export KUBECTL_LOG="$tmp/log"
echo 'kind: ConfigMap' | PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null
seq=$(sequence_of "$KUBECTL_LOG")
[[ $seq == "deps-apply deps-job job app-apply app-rollout" ]] || fail "ordre violé : $seq -- log : $(cat "$KUBECTL_LOG")"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | FAIL_JOB=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un Job en échec n'arrête pas le déploiement"
fi
! grep -q '!gateway.test/phase' "$KUBECTL_LOG" || fail "l'application est appliquée malgré un Job en échec"
echo "gateway-deploy_test: ok"
