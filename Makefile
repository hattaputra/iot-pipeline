.PHONY: build deploy rebuild

build:
	docker build -t iot-api:latest ./api
	docker build -t iot-consumer:latest ./consumer

deploy:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/kafka/statefulset.yaml
	kubectl apply -f k8s/clickhouse/configmap.yaml
	kubectl apply -f k8s/clickhouse/statefulset.yaml
	kubectl apply -f k8s/api/deployment.yaml
	kubectl apply -f k8s/consumer/deployment.yaml
	kubectl apply -f k8s/kafka-exporter/deployment.yaml
	kubectl apply -f k8s/prometheus/configmap.yaml
	kubectl apply -f k8s/prometheus/deployment.yaml
	helm upgrade --install grafana grafana/grafana \
		--namespace monitoring \
		--values k8s/grafana/helm-values.yaml

rebuild: build
	kubectl rollout restart deployment iot-api -n ingestion
	kubectl rollout restart deployment iot-consumer -n ingestion