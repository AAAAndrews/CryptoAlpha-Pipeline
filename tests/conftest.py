"""
tests/conftest.py — pytest 配置 / pytest configuration

统一将项目根目录添加到 sys.path，确保所有测试文件无需各自处理路径。
Add project root to sys.path so all test files can import project modules.
"""
import sys
import os

# 项目根目录 / project root (tests/ 的上一级)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
