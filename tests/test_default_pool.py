"""The process-wide default pool: default_pool / set_default_pool /
using_pool — save/restore semantics, type validation, fork staleness."""

import pytest

import pymizu


@pytest.fixture(autouse=True)
def restore_default():
    prev = pymizu.set_default_pool(None)
    yield
    pymizu.set_default_pool(prev)


@pytest.fixture
def pool():
    p = pymizu.Pool.create(1)
    yield p
    p.stop()


def test_unset_default_is_none():
    assert pymizu.default_pool() is None


def test_set_get_clear_roundtrip(pool):
    assert pymizu.set_default_pool(pool) is None
    assert pymizu.default_pool() is pool
    assert pymizu.set_default_pool(None) is pool
    assert pymizu.default_pool() is None


def test_set_validates_type():
    with pytest.raises(TypeError):
        pymizu.set_default_pool(42)


def test_using_pool_scopes_and_restores(pool):
    other = pymizu.Pool.create(1)
    try:
        pymizu.set_default_pool(pool)
        with pymizu.using_pool(other) as p:
            assert p is other
            assert pymizu.default_pool() is other
        assert pymizu.default_pool() is pool
        with pytest.raises(RuntimeError, match="boom"):
            with pymizu.using_pool(None):
                assert pymizu.default_pool() is None
                raise RuntimeError("boom")
        assert pymizu.default_pool() is pool
    finally:
        other.stop()


def test_forked_child_reads_unset_without_clearing(pool):
    pymizu.set_default_pool(pool)
    pool_, pid = pymizu._default_state
    pymizu._default_state = (pool_, pid + 1)
    assert pymizu.default_pool() is None
    assert pymizu._default_state[0] is pool
    pymizu._default_state = (pool_, pid)
    assert pymizu.default_pool() is pool
