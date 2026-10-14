"""持续维护的真实提交／协调接口回归；仅在临时合成目录运行。"""
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMP = tempfile.TemporaryDirectory(prefix="prx-maintenance-")
COPY = os.path.join(TEMP.name, "app")
shutil.copytree(ROOT, COPY, ignore=shutil.ignore_patterns(
    ".git", "node_modules", "work", "__pycache__", ".env", "*.log",
    "repo_data", "checked_data", "auto_data", "run_logs", "config.local.json"))
sys.path.insert(0, COPY)
import server


def item(n, **fields):
    return {"proxy": f"http://192.0.2.{n}:8080", "grade": "A",
            "target_profile": "generic", "usable": True, **fields}


class RepositoryRegression(unittest.TestCase):
    def test_same_count_manual_replacement_survives_late_snapshot(self):
        token = "same_count"
        old = server.write_repo_data(token, [item(1), item(2)])
        server.save_repo_payload(token, [item(3), item(4)], "replace", 2)
        server.merge_repo_results(token, old, [item(1)], [item(1)["proxy"]], "stable_only")
        self.assertEqual([r["proxy"] for r in server.read_repo_data(token)],
                         [item(3)["proxy"], item(4)["proxy"]],
                         "迟到结果不得恢复被人工替换的旧仓库（数量相同也必须识别）")


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        TEMP.cleanup()
