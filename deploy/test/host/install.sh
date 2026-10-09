#!/usr/bin/env bash
# Préparation d'un VPS Rocky Linux 10 neuf, en root, une fois : k3s, pare-feu, compte de CD.
# usage : install.sh server IFACE "<clé publique ssh de la CD>"
#         install.sh agent IFACE SERVER_IP < node-token     (nœud des dépendances, step-287g)
# IFACE est l'interface du VPC : tout le trafic entre nœuds y passe, jamais par l'IP publique.
set -euo pipefail
usage="usage: $0 server IFACE <clé CD> | agent IFACE SERVER_IP < node-token"
role=${1:?$usage} iface=${2:?$usage}
case $role in
  server) pubkey=${3:?$usage} ;;
  agent) server_ip=${3:?$usage}; token=$(cat); [[ -n $token ]] || { echo "$usage" >&2; exit 1; } ;;
  *) echo "$usage" >&2; exit 1 ;;
esac
node_ip=$(ip -4 -br addr show "$iface" | awk '{sub(/\/.*/, "", $3); print $3}')
[[ -n $node_ip ]] || { echo "install.sh: aucune IPv4 sur $iface" >&2; exit 1; }
here=$(cd "$(dirname "$0")" && pwd)

# kernel-modules-extra (exigé par k3s sur RHEL 10) porte xt_conntrack, xt_comment et br_netfilter. Sans
# version, dnf prend celui du noyau le plus récent, pas du noyau en cours : kube-proxy et flannel
# échouent alors en silence et aucun pod ne joint l'API ni l'extérieur.
dnf install -y -q firewalld container-selinux "kernel-modules-extra-$(uname -r)"
systemctl enable --now firewalld
firewall-cmd --permanent --zone=public --add-service=ssh
if [[ $role == server ]]; then
  firewall-cmd --permanent --zone=public --add-service=http --add-service=https
  firewall-cmd --permanent --zone=public --add-port=2775/tcp
fi
# La zone public de Rocky ouvre cockpit (9090) par défaut.
firewall-cmd --permanent --zone=public --remove-service=cockpit
# Réseaux des pods et des Services de k3s (docs k3s) : sans eux, firewalld coupe le trafic entre pods.
firewall-cmd --permanent --zone=trusted --add-source=10.42.0.0/16 --add-source=10.43.0.0/16
# Le VPC ne relie que nos hôtes : API (6443), kubelet (10250) et vxlan de flannel (8472/udp) y passent.
firewall-cmd --permanent --zone=trusted --change-interface="$iface"
firewall-cmd --reload
# 50-cloud-init.conf pose PasswordAuthentication yes, et le premier réglage lu gagne : un fichier trié
# avant lui (00-) prend la priorité.
cat >/etc/ssh/sshd_config.d/00-gateway.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
EOF
sshd -t
systemctl reload sshd

# Traefik bundlé : ServersTransport.spec.rootCAs (utilisé par ingress.yaml) exige Traefik >= 3.2.
# v1.36.4+k3s1 est la tête du canal "stable" de k3s (update.k3s.io/v1-release/channels) et embarque
# Traefik v3.7.8. INSTALL_K3S_VERSION doit atteindre le script get.k3s.io (le "sh"), pas "curl".
if [[ $role == agent ]]; then
  # La teinte écarte de ce nœud tout ce qui ne la tolère pas : seules les dépendances épinglées par
  # deploy/test/patches/deps-node.yaml y tournent.
  curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.4+k3s1 K3S_URL="https://$server_ip:6443" \
    K3S_TOKEN="$token" sh -s - agent --node-ip "$node_ip" --flannel-iface "$iface" \
    --node-label gateway.test/role=deps --node-taint gateway.test/role=deps:NoSchedule
  echo "install.sh: agent prêt ($node_ip)"
  exit 0
fi
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.4+k3s1 sh -s - --write-kubeconfig-mode 600 \
  --node-ip "$node_ip" --flannel-iface "$iface"
until kubectl get nodes >/dev/null 2>&1; do sleep 2; done
systemctl show k3s -p LimitNOFILE

kubectl create namespace gateway --dry-run=client -o yaml | kubectl apply -f -
# Le Role du compte deploy ne peut pas s'appliquer à lui-même (Namespace est cluster-scoped) : sans
# cette étiquette posée ici, une clé de CD fuitée pourrait créer un pod privilégié ou hostPath/hostNetwork.
kubectl label ns gateway pod-security.kubernetes.io/enforce=baseline --overwrite
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata: {name: deploy, namespace: gateway}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: deploy, namespace: gateway}
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: deploy, namespace: gateway}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: deploy}
subjects: [{kind: ServiceAccount, name: deploy, namespace: gateway}]
---
apiVersion: v1
kind: Secret
metadata:
  name: deploy-token
  namespace: gateway
  annotations: {kubernetes.io/service-account.name: deploy}
type: kubernetes.io/service-account-token
EOF
until token=$(kubectl -n gateway get secret deploy-token -o jsonpath='{.data.token}' | base64 -d) && [[ -n $token ]]; do sleep 1; done

id deploy >/dev/null 2>&1 || useradd -m -s /bin/bash deploy
install -d -m 700 -o deploy -g deploy /home/deploy/.kube /home/deploy/.ssh
KUBECONFIG=/home/deploy/.kube/config kubectl config set-cluster k3s \
  --server=https://127.0.0.1:6443 \
  --certificate-authority=/var/lib/rancher/k3s/server/tls/server-ca.crt --embed-certs
KUBECONFIG=/home/deploy/.kube/config kubectl config set-credentials deploy --token="$token"
KUBECONFIG=/home/deploy/.kube/config kubectl config set-context deploy --cluster=k3s --user=deploy --namespace=gateway
KUBECONFIG=/home/deploy/.kube/config kubectl config use-context deploy
chown deploy:deploy /home/deploy/.kube/config && chmod 600 /home/deploy/.kube/config

install -m 755 "$here/gateway-deploy" /usr/local/bin/gateway-deploy
# restrict (OpenSSH >= 7.2) désactive tout ce qu'une commande forcée peut
# laisser passer par défaut, en un seul mot-clé au lieu d'une énumération qui peut en oublier un.
printf 'restrict,command="/usr/local/bin/gateway-deploy" %s\n' "$pubkey" \
  >/home/deploy/.ssh/authorized_keys
chown deploy:deploy /home/deploy/.ssh/authorized_keys && chmod 600 /home/deploy/.ssh/authorized_keys
echo "install.sh: prêt. Empreinte de l'hôte pour TEST_SSH_KNOWN_HOSTS :"
ssh-keyscan -t ed25519 127.0.0.1 2>/dev/null | sed "s/^127.0.0.1/$(curl -s4 ifconfig.me)/"
