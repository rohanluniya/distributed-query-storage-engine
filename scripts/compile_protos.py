"""Compile schemas/query_engine.proto into services/proto_gen/ Python gRPC stubs."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys


def compile_protos() -> None:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    schema_dir = repo_root / "schemas"
    out_dir = repo_root / "services" / "proto_gen"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "__init__.py").write_text('"""Generated gRPC stubs for WorkerService."""\n')

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{schema_dir}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        str(schema_dir / "query_engine.proto"),
    ]
    subprocess.run(cmd, check=True)

    grpc_file = out_dir / "query_engine_pb2_grpc.py"
    text = grpc_file.read_text()
    # Rewrite top-level import to package-relative import so services.proto_gen works cleanly
    patched = re.sub(
        r"^import query_engine_pb2 as query__engine__pb2$",
        "from services.proto_gen import query_engine_pb2 as query__engine__pb2",
        text,
        flags=re.MULTILINE,
    )
    grpc_file.write_text(patched)
    print(f"Compiled gRPC stubs into {out_dir}")


if __name__ == "__main__":
    compile_protos()
