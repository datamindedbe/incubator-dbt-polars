import threading

from dbt.adapters.polars.compute_slots import ComputeSlots


def test_unlimited_without_configuration():
    slots = ComputeSlots()
    with slots.hold(), slots.releasable(True):
        slots.release()
        slots.ensure_held()


def test_released_slot_is_free_until_the_model_computes_again():
    slots = ComputeSlots()
    slots.configure(1)

    with slots.hold():
        with slots.releasable(True):
            slots.release()
        assert slots.semaphore.acquire(blocking=False)
        slots.semaphore.release()
        slots.ensure_held()
        assert not slots.semaphore.acquire(blocking=False)
    assert slots.semaphore.acquire(blocking=False)


def test_slot_is_kept_unless_releasable():
    slots = ComputeSlots()
    slots.configure(1)

    with slots.hold():
        slots.release()
        with slots.releasable(False):
            slots.release()
        assert slots.is_holding()
        assert not slots.semaphore.acquire(blocking=False)


def test_model_that_finishes_after_a_release_does_not_wait_for_a_slot():
    slots = ComputeSlots()
    slots.configure(1)
    other_model_computing = threading.Event()
    finished = threading.Event()

    def other_model():
        with slots.hold():
            other_model_computing.set()
            finished.wait(timeout=5)

    with slots.hold():
        with slots.releasable(True):
            slots.release()
        thread = threading.Thread(target=other_model)
        thread.start()
        assert other_model_computing.wait(timeout=5)
    finished.set()
    thread.join()


def test_nested_hold_takes_one_slot():
    slots = ComputeSlots()
    slots.configure(1)

    with slots.hold():
        with slots.hold():
            pass
        assert slots.is_holding()
