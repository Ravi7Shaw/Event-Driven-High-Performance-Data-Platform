import random


class InjectedFailure(RuntimeError):
    pass


class Faults:
    """Seedable independent failure points; disabled by default and in production."""

    def __init__(self, rate=0, seed=42):
        self.rate = rate
        self.random = random.Random(seed)

    def check(self, point):
        if self.random.random() < self.rate:
            raise InjectedFailure(point)
