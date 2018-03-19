set -e

# Give the service a moment to finish deploying
sleep 3

cluster_ip=`kubectl get service -n testing nginx -o json | jq -r .spec.clusterIP`
node_port=`kubectl get service -n testing nginx -o json | jq -r .spec.ports[].nodePort`
test_node=`kubectl get nodes -o json | jq -r .items[1].metadata.name`
echo "Testing ${cluster_ip}:${node_port} from ${test_node}"

# These should fail if nginx deployment wasn't successful
curl -s http://${cluster_ip}/index.html | grep 'Hello world'
curl -s http://${cluster_ip}/data2.txt | grep $1
