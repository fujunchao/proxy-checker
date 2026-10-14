"""离线导出当前完整快照供旧版读取；只写全新的输出目录，不修改运行目录。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from repository import encode


def export_snapshot(snapshot_path, output_path):
    source, output = Path(snapshot_path).resolve(), Path(output_path).resolve()
    if output.exists():
        raise ValueError("输出目录必须不存在，禁止覆盖已有备份")
    with source.open("rb") as f:
        envelope = json.load(f)
    data = envelope["data"]
    if (hashlib.sha256(encode(data)).hexdigest() != envelope["sha256"]
            or data["format"] != 1 or (not data.get("legacy_json_missing") and json.loads(data["json"]) != data["items"])):
        raise ValueError("快照校验失败，未导出")
    suffix = ".snapshot.json"
    if not source.name.endswith(suffix):
        raise ValueError("需要 <token>.snapshot.json 权威快照")
    token = source.name[:-len(suffix)]
    output.parent.mkdir(parents=True, exist_ok=True)
    staged = output.with_name(output.name + ".staging-" + uuid.uuid4().hex)
    staged.mkdir()
    # 显式回退导出把 TXT-only 的逻辑库存写成 JSON，避免旧版优先读取空 JSON 而忽略 TXT。
    json_text = json.dumps(data["items"], ensure_ascii=False) if data.get("legacy_json_missing") else data["json"]
    files = {token + ".json": json_text, token + ".txt": data["txt"] or ""}
    manifest = {"repo_revision": data["revision"], "count": len(data["items"]),
                "sha256": {k: hashlib.sha256(v.encode()).hexdigest() for k, v in files.items()}}
    files["manifest.json"] = json.dumps(manifest, ensure_ascii=False, indent=2)
    for name, content in files.items():
        with (staged / name).open("wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
    # 若中途异常，只有 staging 目录；正式输出目录只会包含完整的一对文件。
    os.rename(staged, output)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    manifest = export_snapshot(args.snapshot, args.output)
    print(f"已导出完整版本 {manifest['repo_revision']}，共 {manifest['count']} 条；未修改源目录")
