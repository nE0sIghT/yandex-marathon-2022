import logging
import os
from itertools import permutations
from multiprocessing import Pool, cpu_count

from .truck_processor import TruckProcessor
from .logs import NameAbbrFilter, get_logger


class Input:
    def __init__(self, n: int, m: int, k: int, matrix: tuple[tuple[int]], restrictions: tuple[int]) -> None:
        self._log = get_logger(self)

        self._n = n
        self._m = m
        self._k = k
        self._matrix = matrix
        self._restrictions = restrictions
        self._transfered = 0

    def calculate(self) -> str:
        processor = TruckProcessor()

        if os.getenv('MARATHON_MULTIPROCESS', "") == "1":
            with Pool(cpu_count() - 2) as pool:
                for truck_ids in permutations(range(self._k)):
                    pool.apply_async(
                        processor.iteration,
                        args=(
                            truck_ids,
                            self._n,
                            self._m,
                            self._matrix,
                            self._restrictions
                        ),
                        callback=processor.on_success,
                        error_callback=processor.on_error
                    )

                pool.close()
                pool.join()
        else:
            for truck_ids in permutations(range(self._k)):
                processor.on_success(
                    processor.iteration(truck_ids, self._n, self._m, self._matrix, self._restrictions)
                )

        results = dict(sorted(processor.results().items(), reverse=True))
        self._transfered = next(iter(results))
        routes = dict(sorted(results[self._transfered].items()))

        self._log.info(f"Transfered {self._transfered} of {self._n}")

        return "\n".join(
            " ".join(map(str, [len(route)] + route))
            for route in routes.values()
        )

    def get_transfered(self):
        return self._transfered

def main():
    logging.basicConfig(
        format="%(asctime)s: %(levelname)s %(name_abbr)s %(message)s",
        level=logging.INFO,
        filename="marathon.log",
        filemode="w"
    )
    logging.getLogger().handlers[0].addFilter(NameAbbrFilter())

    total = 0
    for i in range(1, 31):
        if not os.path.exists(f"input/input{i}.txt"):
            break

        with open(f"input/input{i}.txt", "r", encoding="ascii") as fp:
            logging.info(f"Processing input {i}")
            n, m, k = map(int, fp.readline().strip().split(" "))

            matrix: tuple[tuple[int]] = tuple(
                tuple(map(int, fp.readline().strip().split(" ", n + m)[:n + m + 1]))
                for _ in range(n + m + 1)
            )

            restrictions = tuple(map(int, fp.readline().strip().split(" ", k - 1)))

            _input = Input(n, m, k, matrix, restrictions)
            with open(f"output/output{i}.txt", "w") as fo:
                fo.write(_input.calculate())

            total += _input.get_transfered()

    logging.info(f"Total transfered: {total}")
