.PHONY: proto proto-export test typecheck check

# Pinned to the version that produced the vendored protobufs, so
# regeneration does not silently drop the grpc runtime-version guard they carry.
PYTHON_PROTO_TOOLS_VERSION := 1.83.0
PYTHON ?= python3
BUF ?= buf

# Proto source module on the Buf Schema Registry. `proto` exports this
# module's current contents into PROTO_EXPORT_DIR before generating, so this
# SDK never needs a local checkout of the compute repository.
BSR_MODULE ?= buf.build/bonya/tyto
PROTO_EXPORT_DIR ?= .proto-export

# Override to generate from a local checkout instead of BSR, e.g. while
# developing against unpublished proto changes:
#   make proto PROTO_DIR=../../compute/proto
PROTO_DIR ?= $(PROTO_EXPORT_DIR)

proto-export:
	rm -rf $(PROTO_EXPORT_DIR)
	$(BUF) export $(BSR_MODULE) -o $(PROTO_EXPORT_DIR)

# Depends on proto-export only when PROTO_DIR wasn't overridden to a local
# checkout; `make proto PROTO_DIR=../../compute/proto` skips the BSR export.
ifeq ($(PROTO_DIR),$(PROTO_EXPORT_DIR))
proto: proto-export
endif
proto:
	@actual=$$($(PYTHON) -c "from importlib.metadata import version; print(version('grpcio-tools'))" 2>/dev/null); \
	if [ "$$actual" != "$(PYTHON_PROTO_TOOLS_VERSION)" ]; then \
		echo "grpcio-tools==$(PYTHON_PROTO_TOOLS_VERSION) is required (found: $${actual:-missing})." >&2; \
		echo "Install it in a virtualenv and rerun, e.g." >&2; \
		echo "  python3 -m venv .venv && .venv/bin/python -m pip install 'grpcio-tools==$(PYTHON_PROTO_TOOLS_VERSION)'" >&2; \
		echo "  make proto PYTHON=.venv/bin/python" >&2; \
		exit 1; \
	fi
	$(PYTHON) -m grpc_tools.protoc \
		--proto_path=$(PROTO_DIR) \
		--python_out=src/tyto/_proto \
		--grpc_python_out=src/tyto/_proto \
		$(PROTO_DIR)/tyto/runtime/v1/guest.proto \
		$(PROTO_DIR)/tyto/runtime/v1/host.proto \
		$(PROTO_DIR)/tyto/runtime/v1/preview.proto \
		$(PROTO_DIR)/tyto/runtime/v1/tapi.proto
	$(PYTHON) scripts/rewrite-python-proto-imports.py src/tyto/_proto

test:
	$(PYTHON) -m pytest

# The package is checked strictly via [tool.mypy] in pyproject.toml; examples/
# is passed explicitly because it is outside the configured packages list and
# would otherwise never be checked at all.
typecheck:
	$(PYTHON) -m mypy
	$(PYTHON) -m mypy --strict examples/

check: typecheck test
