import logging
import random
import os
from enum import Enum
from itertools import islice, permutations
from multiprocessing import Process, Manager
from typing import Any, Optional

from .logs import NameAbbrFilter, get_logger


class PointRestriction(Enum):
    N = 0
    M = 1

    @staticmethod
    def get(cargo: int, capacity: int):
        if cargo == 0:
            return PointRestriction.N

        if cargo == capacity:
            return PointRestriction.M

        return None


class Matrix:
    def __init__(self, n: int, m: int, matrix: tuple[tuple[int]]) -> None:
        self._n = n
        self._m = m
        self._used_n = 0
        self._matrix = matrix
        self._distances: dict[int, dict[int, list[int]]] = {}

        # Кешируем кратчайшие пути
        for point, point_distances in enumerate(self._matrix):
            for point2, distance in enumerate(point_distances):
                if point == point2 or point2 == 0:
                    continue

                self._distances.setdefault(point, {}).setdefault(distance, []).append(point2)

            self._distances[point] = dict(sorted(self._distances[point].items()))

            for point_distances in self._distances[point].values():
                point_distances.sort()

        self._used: set[int] = set()

    def is_used(self, point: int):
        return point in self._used

    def is_n_point(self, point: int):
        return point > 0 and point <= self._n

    def is_m_point(self, point: int):
        return point > self._n and point <= self._n + self._m

    def use(self, point: int):
        if self.is_used(point):
            raise Exception(f"Point {point} already used")

        if self.is_n_point(point):
            self._used_n += 1

        self._used.add(point)

    def free(self, point: int):
        if self.is_n_point(point):
            self._used_n -= 1

        self._used.remove(point)

    def get_n(self):
        return self._n

    def get_used_n_points(self):
        return self._used_n

    def have_free_n_points(self):
        return self._used_n < self._n

    def nearest_point(self, point: int, maximum_distance: int, point_restriction: Optional[PointRestriction]):
        def point_allowed(target: int):
            if point_restriction is None:
                return True

            return (
                point_restriction == PointRestriction.N and self.is_n_point(target)
            ) or (
                point_restriction == PointRestriction.M and self.is_m_point(target)
            )

        distance, targets = next(
            (
                distance,
                tuple(
                    target
                    for target in targets
                    if not self.is_used(target) and point_allowed(target)
                )
            )
            for distance, targets in self._distances[point].items()
            if distance <= maximum_distance and
            any(
                target
                for target in targets
                if not self.is_used(target) and point_allowed(target)
            )
        )

        if not targets:
            raise StopIteration()

        # Мне честно лень писать перебор всех путей
        # Попробую положиться на случай
        return (random.choice(targets), distance)

    def distance(self, point1: int, point2: int):
        return self._matrix[point1][point2]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._n}, {self._m}, {self._matrix}"

    def __str__(self) -> str:
        return f"[{self._n}, {self._m}]: {self._matrix}"


class Truck:
    class Order(Enum):
        FUEL = 0
        INDEX = 1

    def __init__(self, k: int, fuel: int, matrix: Matrix) -> None:
        self._log = get_logger(self)

        self._k = k
        self._fuel = fuel
        self._distance = 0
        self._matrix = matrix

        self._route: list[int] = []
        self._done = False

        self._cargo = 0
        self._capacity = 25
        self._unloading = False
        self._safepoint = 0

        self.order_by(self.Order.FUEL)

    def calculate(self):
        if self._done:
            return

        while not self._done:
            try:
                if self._cargo == 0:
                    if not self._matrix.have_free_n_points():
                        raise StopIteration()

                    if self._unloading:
                        if self._safepoint != self.get_position():
                            self._safepoint = self.get_position()
                            self._unloading = False
                        else:
                            raise StopIteration()

                point, distance = self._matrix.nearest_point(
                    self.get_position(),
                    self._fuel - self._distance,
                    PointRestriction.get(
                        self._cargo, self._capacity)
                        if not self._unloading
                        else PointRestriction.M
                )
                self._add_route_point(point, distance)

                if self._unloading:
                    self._unloading = False
            except StopIteration:
                if self._cargo == 0:
                    self._done = True
                    break

                self._unloading = True
                self._unload_one()

    def get_position(self):
        try:
            return self._route[-1]
        except IndexError:
            return 0

    def get_index(self):
        return self._k

    def get_fuel(self):
        return self._fuel

    def get_route(self):
        return self._route

    def order_by(self, order: Order):
        self._order = order

    def _point_type(self, point: int):
        if self._matrix.is_n_point(point):
            return "N"
        else:
            return "M"

    def _add_route_point(self, point: int, distance: int):
        self._route.append(point)
        self._distance += distance

        if self._matrix.is_n_point(point):
            self._cargo += 1
        else:
            self._cargo -= 1

        self._matrix.use(point)

        self._log.debug(
            f"Truck {self._k} used {self._point_type(point)} point {point}. Cargo {self._cargo}, distance {self._distance}/{self._fuel}"
        )

    def _unload_one(self):
        while True:
            try:
                position = self._route.pop()
            except IndexError:
                raise IndexError("Unloaded until start position")

            self._matrix.free(position)
            self._distance -= self._matrix.distance(self.get_position(), position)
            if self._matrix.is_m_point(position):
                self._cargo += 1
            else:
                self._cargo -= 1

            self._log.debug(f"Truck {self._k} freed {self._point_type(position)} point {position}. Cargo {self._cargo}, distance {self._distance}/{self._fuel}")

            if self._matrix.is_n_point(position):
                break

    def __lt__(self, other: Any):
        if not isinstance(other, Truck):
            raise NotImplemented()

        if self._order == self.Order.FUEL:
            return self._fuel < other.get_fuel()
        else:
            return self._k < other.get_index()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._k}, {self._fuel}, {self._matrix}"

    def __str__(self) -> str:
        return f"[{self._k}]: {self._fuel}"


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
        def calculate_iteration(truck_ids: tuple[int], results: dict[int, dict[int, list[int]]]):
            matrix = Matrix(self._n, self._m, self._matrix)
            truck_routes: dict[int, list[int]] = {}
            for index in truck_ids:
                truck = Truck(index, self._restrictions[index], matrix)
                truck.calculate()
                truck_routes[index] = truck.get_route()

            results[matrix.get_used_n_points()] = truck_routes

        manager = Manager()
        results: dict[int, dict[int, list[int]]] = manager.dict()
        
        for truck_ids in permutations(range(self._k)):
            tasks: set[Process] = set()

            # На 5800x можно и побрутфорсить :-)
            for _ in range(3):
                for _ in range(15):
                    process = Process(target=calculate_iteration, args=(truck_ids, results))
                    process.start()
                    tasks.add(process)

                for task in tasks:
                    task.join()

        results = dict(sorted(results.items(), reverse=True))
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
