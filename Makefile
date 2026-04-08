.PHONY: build deploy rebuild helm-repos import-images

build:
	docker build -t iot-api:latest ./api
	docker build -t iot-consumer:latest ./consumer
	docker build -t event-detector:latest ./event-detector

helm-repos:
	helm repo add grafana https://grafana.github.io/helm-charts
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update

import-images:
	docker save iot-api:latest | k3s ctr images import -
	docker save iot-consumer:latest | k3s ctr images import -
	docker save event-detector:latest | k3s ctr images import -

deploy: helm-repos
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/kafka/statefulset.yaml
	kubectl apply -f k8s/clickhouse/configmap.yaml
	kubectl apply -f k8s/clickhouse/statefulset.yaml
	kubectl apply -f k8s/redis/configmap.yaml
	kubectl apply -f k8s/redis/statefulset.yaml
	kubectl apply -f k8s/api/deployment.yaml
	kubectl apply -f k8s/consumer/deployment.yaml
	kubectl apply -f k8s/event-detector/deployment.yaml
	kubectl apply -f k8s/kafka-exporter/deployment.yaml
	kubectl apply -f k8s/prometheus/configmap.yaml
	kubectl apply -f k8s/prometheus/deployment.yaml
	kubectl apply -f k8s/ingress.yaml
	helm upgrade --install grafana grafana/grafana \
		--namespace monitoring \
		--values k8s/grafana/helm-values.yaml
	helm upgrade --install kube-state-metrics prometheus-community/kube-state-metrics \
		--namespace monitoring

rebuild: build import-images
	kubectl rollout restart deployment iot-api -n ingestion
	kubectl rollout restart deployment iot-consumer -n ingestion
	kubectl rollout restart deployment event-detector -n ingestion
