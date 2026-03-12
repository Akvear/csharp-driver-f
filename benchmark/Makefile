SCYLLA_URI ?= 172.17.0.2:9042
ITERATIONS ?= 10000

DATASTAX_DIR   := datastax-csharp-driver
NEW_CSHARP_DIR := new-csharp-driver
RUST_DIR       := rust-driver
COMPOSE_DIR    := docker-composes

DOTNET_DRIVERS := datastax new-csharp
ALL_DRIVERS    := datastax new-csharp rust

DRIVER_DIR_datastax   := $(DATASTAX_DIR)
DRIVER_DIR_new-csharp := $(NEW_CSHARP_DIR)
DRIVER_DIR_rust       := $(RUST_DIR)

.PHONY: all build clean help \
        insert select concurrent_insert \
        docker-up docker-down

all: build


# Helpers
define run_dotnet
	cd $(1) && SCYLLA_URI=$(SCYLLA_URI) ITERATIONS=$(ITERATIONS) BENCHMARK=$(2) dotnet run -c Release
endef

define run_rust
	cd $(1) && SCYLLA_URI=$(SCYLLA_URI) ITERATIONS=$(ITERATIONS) BENCHMARK=$(2) cargo run --release
endef

define run_driver
	@echo "=== $(2): $(1)-driver ==="
	$(if $(filter rust,$(1)), \
		$(call run_rust,$(DRIVER_DIR_$(1)),$(2)), \
		$(call run_dotnet,$(DRIVER_DIR_$(1)),$(2)) \
	)
	@echo ""
endef

# Build
build: $(addprefix build-,$(ALL_DRIVERS))

build-datastax:
	cd $(DATASTAX_DIR) && dotnet build -c Release

build-new-csharp:
	cd $(NEW_CSHARP_DIR) && dotnet build -c Release

build-rust:
	cd $(RUST_DIR) && cargo build --release

# Benchmarks (all drivers)
insert:
	$(foreach d,$(ALL_DRIVERS),$(call run_driver,$(d),insert))

select:
	$(foreach d,$(DOTNET_DRIVERS),$(call run_driver,$(d),select))

concurrent_insert:
	$(foreach d,$(DOTNET_DRIVERS),$(call run_driver,$(d),concurrent_insert))

# Per-driver targets
define make_driver_target
$(1)-$(2):
	$(call run_driver,$(2),$(1))
endef

$(foreach bench,insert select concurrent_insert, \
  $(foreach drv,$(ALL_DRIVERS), \
    $(eval $(call make_driver_target,$(bench),$(drv))) \
))

# Docker
docker-up: docker-up-scylla-1node-1shard
docker-down: docker-down-scylla-1node-1shard

define docker_target
docker-up-$(1):
	docker compose -f $(COMPOSE_DIR)/$(2) up -d
	@echo "$(3)"
	@echo "Run with: make insert SCYLLA_URI=$(4)"

docker-down-$(1):
	docker compose -f $(COMPOSE_DIR)/$(2) down
endef

$(eval $(call docker_target,scylla-1node-1shard,scylla-1node-1shard.yml,\
"ScyllaDB 1-node / 1-shard ready",172.47.0.2:9042))

$(eval $(call docker_target,scylla-3nodes-1shard,scylla-3nodes-1shard.yml,\
"ScyllaDB 3-node / 1-shard ready",172.46.0.2:9042))

$(eval $(call docker_target,scylla-3nodes-2shards,scylla-3nodes-2shards.yml,\
"ScyllaDB 3-node / 2-shards ready",172.45.0.2:9042))

$(eval $(call docker_target,cassandra-single,cassandra-single-node.yml,\
"Cassandra single node ready",172.43.0.2:9042))

$(eval $(call docker_target,cassandra-three-nodes,cassandra-three-nodes.yml,\
"Cassandra 3-node cluster ready",172.44.0.2:9042))

clean:
	cd $(DATASTAX_DIR) && dotnet clean
	cd $(NEW_CSHARP_DIR) && dotnet clean
	cd $(RUST_DIR) && cargo clean

# Help
help:
	@echo ""
	@echo "Usage:"
	@echo "  make [TARGET] [SCYLLA_URI=host:port] [ITERATIONS=N]"
	@echo ""
	@echo "Benchmarks:"
	@echo "  insert              run INSERT benchmark"
	@echo "  select              run SELECT benchmark"
	@echo "  concurrent_insert   run parallel INSERT benchmark"
	@echo ""
	@echo "Build:"
	@echo "  build               build all drivers"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up                         start default cluster"
	@echo "  docker-up-scylla-1node-1shard"
	@echo "  docker-up-scylla-3nodes-1shard"
	@echo "  docker-up-scylla-3nodes-2shards"
	@echo "  docker-up-cassandra-single"
	@echo "  docker-up-cassandra-three-nodes"
	@echo ""
	@echo "Defaults:"
	@echo "  SCYLLA_URI=$(SCYLLA_URI)"
	@echo "  ITERATIONS=$(ITERATIONS)"