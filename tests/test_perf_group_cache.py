"""
tests/test_perf_group_cache.py — Task 2: group_labels 缓存机制测试
Tests for Task 2: FactorEvaluator group_labels cache mechanism.

验证项 / Verifications:
- 初始化时 _cached_group_labels 为 None
- run_grouping() 后缓存自动设置且与 group_labels 一致
- _set_group_cache / _clear_group_cache 方法正确
- 全量模式和分块模式均正确缓存
- 缓存值是同一引用（非拷贝）
"""

import sys
import os
import pandas as pd

# 确保项目根目录在 sys.path / ensure project root is in sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from FactorAnalysis.evaluator import FactorEvaluator
from tests.mutual_components.conftest_perf import (
    make_synthetic_data, iter_scenarios, assert_series_close,
)


def _make_evaluator(factor, returns, **kwargs):
    """创建 FactorEvaluator 实例 / Create FactorEvaluator instance."""
    return FactorEvaluator(factor, returns, **kwargs)


class TestGroupCacheInit:
    """缓存初始化测试 / Cache initialization tests."""

    def test_cache_none_on_init(self):
        """初始化时 _cached_group_labels 为 None / cache is None on init."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        assert ev._cached_group_labels is None

    def test_group_labels_none_on_init(self):
        """初始化时 group_labels 为 None / group_labels is None on init."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        assert ev.group_labels is None


class TestGroupCacheAfterRunGrouping:
    """run_grouping 后缓存状态测试 / Cache state after run_grouping tests."""

    def test_cache_set_after_full_mode(self):
        """全量模式 run_grouping 后缓存自动设置 / cache auto-set after full mode run_grouping."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        assert ev._cached_group_labels is not None
        assert_series_close(ev._cached_group_labels, ev.group_labels, label="full_mode_cache")

    def test_cache_set_after_chunked_mode(self):
        """分块模式 run_grouping 后缓存自动设置 / cache auto-set after chunked mode run_grouping."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns, chunk_size=50)
        ev.run_grouping()
        assert ev._cached_group_labels is not None
        assert_series_close(ev._cached_group_labels, ev.group_labels, label="chunked_mode_cache")

    def test_cache_is_same_reference(self):
        """缓存与 group_labels 是同一对象引用 / cache is same object reference as group_labels."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        assert ev._cached_group_labels is ev.group_labels

    def test_cache_all_scenarios(self):
        """所有预定义场景缓存正确 / cache correct for all predefined scenarios."""
        checks = 0
        for sid, factor, returns in iter_scenarios():
            ev = _make_evaluator(factor, returns)
            ev.run_grouping()
            assert ev._cached_group_labels is not None, f"[{sid}] cache is None"
            assert_series_close(
                ev._cached_group_labels, ev.group_labels,
                label=f"scenario_{sid}",
            )
            checks += 1
        print(f"[test_cache_all_scenarios] {checks} scenarios passed")


class TestSetClearCache:
    """_set_group_cache / _clear_group_cache 方法测试 / set/clear cache method tests."""

    def test_set_cache_manual(self):
        """手动设置缓存 / manual cache set."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        dummy = pd.Series([1.0, 2.0, 3.0])
        ev._set_group_cache(dummy)
        assert ev._cached_group_labels is dummy

    def test_clear_cache(self):
        """清除缓存后为 None / cache is None after clear."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        assert ev._cached_group_labels is not None
        ev._clear_group_cache()
        assert ev._cached_group_labels is None

    def test_clear_does_not_affect_group_labels(self):
        """清除缓存不影响 group_labels / clear cache does not affect group_labels."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        cached = ev.group_labels
        ev._clear_group_cache()
        assert ev.group_labels is cached  # group_labels 仍存在 / group_labels still exists

    def test_set_cache_overwrite(self):
        """重复设置缓存覆盖旧值 / repeated set overwrites old cache."""
        factor, returns = make_synthetic_data()
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        first_cache = ev._cached_group_labels
        # 重新运行 run_grouping 应覆盖缓存 / re-run should overwrite cache
        ev.run_grouping()
        assert ev._cached_group_labels is not None
        # 数值应一致（相同数据） / values should be consistent (same data)
        assert_series_close(ev._cached_group_labels, first_cache, label="overwrite_consistent")


class TestChunkedCacheIndependence:
    """分块模式缓存独立性测试 / Chunked mode cache independence tests."""

    def test_chunked_different_sizes(self):
        """不同 chunk_size 缓存均与 group_labels 一致 / cache consistent for different chunk_sizes."""
        factor, returns = make_synthetic_data(n_days=200, n_symbols=50)
        for chunk_size in [10, 50, 100, 200]:
            ev = _make_evaluator(factor, returns, chunk_size=chunk_size)
            ev.run_grouping()
            assert ev._cached_group_labels is not None, f"chunk_size={chunk_size}: cache is None"
            assert_series_close(
                ev._cached_group_labels, ev.group_labels,
                label=f"chunk_size_{chunk_size}",
            )

    def test_chunked_vs_full_consistent(self):
        """分块模式与全量模式缓存数值一致 / chunked cache matches full mode."""
        factor, returns = make_synthetic_data(n_days=200, n_symbols=50, seed=42)
        ev_full = _make_evaluator(factor, returns)
        ev_full.run_grouping()

        ev_chunked = _make_evaluator(factor, returns, chunk_size=50)
        ev_chunked.run_grouping()

        assert_series_close(
            ev_full._cached_group_labels, ev_chunked._cached_group_labels,
            label="chunked_vs_full_cache",
        )


if __name__ == "__main__":
    pytest_main = __import__("pytest").main
    pytest_main([__file__, "-v"])
