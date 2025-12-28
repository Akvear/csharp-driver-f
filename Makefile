SHELL := bash

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_VERSION ?= release:2025.2

CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= d3225ac6565242b231129e0c4f8f0b7a041219cf

CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

SCYLLA_EXT_OPTS ?= --smp=2 --memory=4G
SIMULACRON_PATH ?= ${MAKEFILE_PATH}/ci/simulacron-standalone-0.12.0.jar

TARGET_FRAMEWORK ?=
SNK_FILE ?=
DEV_SNK_PUBLIC_KEY ?= 0024000004800000940000000602000000240000525341310004000001000100fb083dc01ba81b96b526327f232e7f4c1301c8ec177a2c66adecc315a9c2308f33ecd9dc70d6d1435107578b4dd04658c8f92a51a60d50c528ca6fba3955fa844fe79c884452024b0ba67d19a70140818aa61a1faeb23d5dcfe0bd9820d587829caf36d0ac7e0dc450d3654d5f5bee009dda3d11fd4066d4640b935c2ca048a4

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

TEST_TARGET_OPTIONS ?=
ifeq (${TEST_TARGET_OPTIONS},)
	ifeq (${TARGET_FRAMEWORK},)
		TEST_TARGET_OPTIONS =
	else ifeq (${TARGET_FRAMEWORK},all)
		TEST_TARGET_OPTIONS = -p:BuildTarget=all
	else
		TEST_TARGET_OPTIONS = -p:BuildTarget=${TARGET_FRAMEWORK} --framework=${TARGET_FRAMEWORK}
	endif
endif

export SCYLLA_EXT_OPTS
export SIMULACRON_PATH
export SCYLLA_VERSION

.PHONY: check
check: check-csharp check-rust

.PHONY: check-csharp
check-csharp:
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj & \
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra/Cassandra.csproj & \
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra.Tests/Cassandra.Tests.csproj & \
	wait

.PHONY: check-rust
check-rust:
	cd rust; cargo fmt -- --check && cargo clippy --all-targets --all-features -- -D warnings

.PHONY: fix
fix: fix-csharp fix-rust

.PHONY: fix-csharp
fix-csharp:
	dotnet format --severity warn --verbosity diagnostic src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj & \
	dotnet format --severity warn --verbosity diagnostic src/Cassandra/Cassandra.csproj & \
	dotnet format --severity warn --verbosity diagnostic src/Cassandra.Tests/Cassandra.Tests.csproj & \
	wait

.PHONY: fix-rust
fix-rust:
	cd rust; cargo fmt && cargo fix && cargo clippy --all-targets --all-features --fix

.PHONY: test-unit
test-unit: .use-development-snk
	dotnet test $(TEST_TARGET_OPTIONS) src/Cassandra.Tests/Cassandra.Tests.csproj

TEST_INTEGRATION_SCYLLA_FILTER ?= (FullyQualifiedName!~ClientWarningsTests & FullyQualifiedName!~CustomPayloadTests & FullyQualifiedName!~Connect_With_Ssl_Test & FullyQualifiedName!~Should_UpdateHosts_When_HostIpChanges & FullyQualifiedName!~Should_UseNewHostInQueryPlans_When_HostIsDecommissionedAndJoinsAgain & FullyQualifiedName!~Should_RemoveNodeMetricsAndDisposeMetricsContext_When_HostIsRemoved & FullyQualifiedName!~Virtual_Keyspaces_Are_Included & FullyQualifiedName!~Virtual_Table_Metadata_Test & FullyQualifiedName!~SessionAuthenticationTests & FullyQualifiedName!~TypeSerializersTests & FullyQualifiedName!~Custom_MetadataTest & FullyQualifiedName!~LinqWhere_WithVectors & FullyQualifiedName!~SimpleStatement_With_No_Compact_Enabled_Should_Reveal_Non_Schema_Columns & FullyQualifiedName!~SimpleStatement_With_No_Compact_Disabled_Should_Not_Reveal_Non_Schema_Columns & FullyQualifiedName!~ColumnClusteringOrderReversedTest & FullyQualifiedName!~GetMaterializedView_Should_Refresh_View_Metadata_Via_Events & FullyQualifiedName!~MaterializedView_Base_Table_Column_Addition & FullyQualifiedName!~MultipleSecondaryIndexTest & FullyQualifiedName!~RaiseErrorOnInvalidMultipleSecondaryIndexTest & FullyQualifiedName!~TableMetadataAllTypesTest & FullyQualifiedName!~TableMetadataClusteringOrderTest & FullyQualifiedName!~TableMetadataCollectionsSecondaryIndexTest & FullyQualifiedName!~TableMetadataCompositePartitionKeyTest & FullyQualifiedName!~TupleMetadataTest & FullyQualifiedName!~Udt_Case_Sensitive_Metadata_Test & FullyQualifiedName!~UdtMetadataTest & FullyQualifiedName!~Should_Retrieve_Table_Metadata & FullyQualifiedName!~CreateTable_With_Frozen_Key & FullyQualifiedName!~CreateTable_With_Frozen_Udt & FullyQualifiedName!~CreateTable_With_Frozen_Value & FullyQualifiedName!~Should_AllMetricsHaveValidValues_When_AllNodesAreUp & FullyQualifiedName!~SimpleStatement_Dictionary_Parameters_CaseInsensitivity_ExcessOfParams & FullyQualifiedName!~SimpleStatement_Dictionary_Parameters_CaseInsensitivity_NoOverload & FullyQualifiedName!~TokenAware_TransientReplication_NoHopsAndOnlyFullReplicas & FullyQualifiedName!~GetFunction_Should_Return_Most_Up_To_Date_Metadata_Via_Events & FullyQualifiedName!~LargeDataTests & FullyQualifiedName!~MetadataTests & FullyQualifiedName!~MultiThreadingTests & FullyQualifiedName!~PoolTests & FullyQualifiedName!~PrepareLongTests & FullyQualifiedName!~SpeculativeExecutionLongTests & FullyQualifiedName!~StressTests & FullyQualifiedName!~TransitionalAuthenticationTests & FullyQualifiedName!~ProxyAuthenticationTests & FullyQualifiedName!~CloudIntegrationTests & FullyQualifiedName!~CoreGraphTests & FullyQualifiedName!~GraphTests & FullyQualifiedName!~InsightsIntegrationTests & FullyQualifiedName!~DateRangeTests & FullyQualifiedName!~FoundBugTests & FullyQualifiedName!~GeometryTests & FullyQualifiedName!~LoadBalancingPolicyTests & FullyQualifiedName!~ConsistencyTests & FullyQualifiedName!~LoadBalancingPolicyTests & FullyQualifiedName!~ReconnectionPolicyTests & FullyQualifiedName!~RetryPolicyTests)
TEST_INTEGRATION_OPTIONS ?= -l "console;verbosity=detailed"
TEST_INTEGRATION_CSPROJ ?= src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj
.PHONY: test-integration-scylla
test-integration-scylla: .use-development-snk .prepare-scylla-ccm
	CCM_DISTRIBUTION=scylla dotnet test $(TEST_TARGET_OPTIONS) $(TEST_INTEGRATION_CSPROJ) $(TEST_INTEGRATION_OPTIONS)

.PHONY: test-integration-cassandra
test-integration-cassandra: .use-development-snk .prepare-cassandra-ccm
	CCM_DISTRIBUTION=cassandra dotnet test $(TEST_TARGET_OPTIONS) $(TEST_INTEGRATION_CSPROJ) $(TEST_INTEGRATION_OPTIONS)

.prepare-cassandra-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"; \
  	else \
		echo "Installing Cassandra CCM ${CCM_CASSANDRA_VERSION}"; \
		pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

.PHONY: install-cassandra-ccm
install-cassandra-ccm:
	@echo "Install CCM ${CCM_CASSANDRA_VERSION}"
	@pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Scylla CCM ${CCM_SCYLLA_VERSION} is already installed"; \
  	else \
		echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"; \
		pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

.PHONY: install-scylla-ccm
install-scylla-ccm:
	@echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"
	@pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.use-development-snk:
	@[ -f build/scylladb.snk ] || ( cp -f build/scylladb-dev.snk build/scylladb.snk )

.prepare-sn-devel:
	if ! sn -h sn 2>/dev/null >&2; then \
  		sudo apt-get update; \
		sudo apt-get install -y mono-devel;\
	fi

.use-production-snk: .prepare-sn-devel
	@if [ -z "${SNK_FILE}" ]; then \
 		echo "Environment variable SNK_FILE is not set. Please set it to the path of the production SNK file."; \
 		exit 1; \
 	else \
 		echo "${SNK_FILE}" | base64 --decode > build/scylladb.snk; \
 	fi; \
 	sn -p build/scylladb.snk /tmp/scylladb.pub; \
 	export PROD_SNK_PUBLIC_KEY=`hexdump -v -e '/1 "%02x"' /tmp/scylladb.pub`; \
 	echo "Switching to production SNK public key: $${PROD_SNK_PUBLIC_KEY}"; \
 	for file in `grep --exclude=Makefile -rIl 'PublicKey=' .`; do \
 	  echo "Processing file: $$file"; \
 	  grep 'PublicKey=$${PROD_SNK_PUBLIC_KEY}' "$$file" 2>/dev/null >&1 || sed -i "s/PublicKey=${DEV_SNK_PUBLIC_KEY}/PublicKey=$${PROD_SNK_PUBLIC_KEY}/g" "$$file" 2> /dev/null 1>&2; \
 	done;

.target-to-dry-run-package:
	grep '<PackageId>ScyllaDBCSharpDriver.DRYRUN' $(PROJECT_PATH) || \
	sed -Ei "s#<PackageId>ScyllaDBCSharpDriver(.*)</PackageId>#<PackageId>ScyllaDBCSharpDriver.DRYRUN\1</PackageId>#g" $(PROJECT_PATH)

.publish-proj-nuget: .use-production-snk
ifndef DRY_RUN
	@echo "Publishing to NuGet with production SNK"
else
	@echo "Dry run publishing to NuGet with production SNK"
	$(MAKE) .target-to-dry-run-package
endif
	dotnet restore $(PROJECT_PATH)
	dotnet build $(PROJECT_PATH) --configuration Release --no-restore
	dotnet pack $(PROJECT_PATH) --configuration Release --no-build --output ./nupkgs
ifndef SKIP_DUPLICATE
	dotnet nuget push "./nupkgs/*.nupkg" --api-key ${NUGET_API_KEY} --source https://api.nuget.org/v3/index.json
else
	dotnet nuget push --skip-duplicate "./nupkgs/*.nupkg" --api-key ${NUGET_API_KEY} --source https://api.nuget.org/v3/index.json
endif

.PHONY: publish-nuget
publish-nuget:
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Cassandra/Cassandra.csproj
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Extensions/Cassandra.AppMetrics/Cassandra.AppMetrics.csproj
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Extensions/Cassandra.OpenTelemetry/Cassandra.OpenTelemetry.csproj

.PHONY: publish-nuget-dry-run
publish-nuget-dry-run:
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Cassandra/Cassandra.csproj DRY_RUN=1
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Extensions/Cassandra.AppMetrics/Cassandra.AppMetrics.csproj DRY_RUN=1
	$(MAKE) .publish-proj-nuget PROJECT_PATH=src/Extensions/Cassandra.OpenTelemetry/Cassandra.OpenTelemetry.csproj DRY_RUN=1

.PHONY: clean
clean: clean-csharp clean-rust

.PHONY: clean-csharp
clean-csharp:
	find . -name '*.csproj' -print0 | xargs -0 -n1 dotnet clean

.PHONY: clean-rust
clean-rust:
	cd rust; cargo clean

### SYMLINK RUST LIBRARY INTO C# PROJECTS ###
# This is inherently hacky and brittle, but it's the easiest way I've found to get the Rust
# library loaded by the C# projects during testing and execution.

LINK_PROJECTS 	 := Cassandra Cassandra.IntegrationTests Cassandra.Tests
FRAMEWORKS    	 := net9 net8

define symlink-to-rust
	mkdir -p $(1); \
	cd $(1); \
	ln -f -s ../../../../../rust/target/debug/libcsharp_wrapper.so || true; \
	cd - >/dev/null
endef

TARGET_DIRS := $(foreach p,$(LINK_PROJECTS), \
                 $(foreach f,$(FRAMEWORKS), \
                   src/$(p)/bin/Debug/$(f)))

.PHONY: create-rust-lib-symlinks
create-rust-lib-symlinks:
	@$(foreach d,$(TARGET_DIRS), \
		echo "Linking in $(d)"; \
		$(call symlink-to-rust,$(d));)

### END SYMLINK ###

# TODO: Put --release for production builds
.PHONY: build-rust
build-rust:
	cd rust; \
	cargo build; \
	cd ../examples/RustWrapper/bin/Debug/net9/; \
	ln -f -s ../../../../../rust/target/debug/libcsharp_wrapper.so . || true

.PHONY: build-rust-asan
build-rust-asan:
	cd rust; \
	RUSTFLAGS="\
		${RUSTFLAGS} \
		-Zsanitizer=address \
		-C link-arg=-Wl,--whole-archive \
		-C link-arg=/usr/lib/clang/20/lib/x86_64-redhat-linux-gnu/libclang_rt.asan_static.a" \
	cargo +nightly build -Zbuild-std --target x86_64-unknown-linux-gnu; \
	cd ../examples/RustWrapper/bin/Debug/net9/ ; \
	ln -f -s ../../../../../rust/target/x86_64-unknown-linux-gnu/debug/libcsharp_wrapper.so . || true

.PHONY: run-wrapper-example
run-wrapper-example:
	dotnet run --project examples/RustWrapper/RustWrapper.csproj

.PHONY: run-wrapper-example-asan
run-wrapper-example-asan:
	LD_PRELOAD=/usr/lib64/libasan.so.8 dotnet run --project examples/RustWrapper/RustWrapper.csproj
