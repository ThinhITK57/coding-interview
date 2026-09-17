import time
from contextlib import contextmanager


class StepTimer:

    def __init__(self):
        self.results = {}

    @contextmanager
    def measure(self, step_name: str):
        start = time.perf_counter()
        yield
        end = time.perf_counter()
        duration = end - start
        self.results[step_name] = duration
        print(f"[TIMER] {step_name}: {duration:.4f}s")

    def summary(self):
        total = sum(self.results.values())
        print("\n========== PERFORMANCE SUMMARY ==========")
        for k, v in self.results.items():
            print(f"{k:<20}: {v:.4f}s")
        print("-----------------------------------------")
        print(f"{'TOTAL':<20}: {total:.4f}s")
        print("=========================================\n")