from enum import Enum
from itertools import product
from typing import Any, Optional, Sequence

from .logs import get_logger


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
    CLOSEST_DISTANCE_RANGE = 10

    def __init__(self, n: int, m: int, matrix: tuple[tuple[int]]) -> None:
        self._n = n
        self._m = m
        self._used_n = 0
        self._matrix = matrix
        self._distances: dict[int, dict[int, list[int]]] = {}
        self._closest_distances: dict[int, tuple[int]] = {}

        # Кешируем кратчайшие пути
        for point, point_distances in enumerate(self._matrix):
            for point2, distance in enumerate(point_distances):
                if point == point2 or point2 == 0:
                    continue

                self._distances.setdefault(point, {}).setdefault(distance, []).append(point2)

            self._distances[point] = dict(sorted(self._distances[point].items()))

            for point_distances in self._distances[point].values():
                point_distances.sort()

        for point, distances in self._distances.items():
            _closest = next(iter(distances.keys()))
            self._closest_distances[point] = tuple(
                d for d in distances.keys()
                if d <= self.CLOSEST_DISTANCE_RANGE * _closest
            )

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

    def get_initial_closest_distances(self):
        distances: list[int] = []

        _closest = None
        next(
            distance
            for distance, targets in self._distances[0].items()
            if any(t for t in targets if not self.is_used(t))
        )

        for distance, targets in self._distances[0].items():
            if _closest is None:
                if any(t for t in targets if not self.is_used(t)):
                    _closest = distance

                    if _closest == self._closest_distances[0][0]:
                        return self._closest_distances[0]
                else:
                    continue

            if distance > _closest * self.CLOSEST_DISTANCE_RANGE:
                break

            distances.append(distance)

        return distances

    def nearest_point(
        self,
        point: int,
        maximum_distance: int,
        point_restriction: Optional[PointRestriction],
        force_distance: Optional[int]
    ):
        def point_allowed(target: int):
            if point_restriction is None:
                return True

            return (
                point_restriction == PointRestriction.N and self.is_n_point(target)
            ) or (
                point_restriction == PointRestriction.M and self.is_m_point(target)
            )

        if force_distance is not None:
            distance_generator = (
                (distance, targets)
                for distance, targets in self._distances[point].items()
                if distance == force_distance
            )
        else:
            distance_generator = (
                d for d in self._distances[point].items()
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
            for distance, targets
            in distance_generator
            if distance <= maximum_distance and
            any(
                target
                for target in targets
                if not self.is_used(target) and point_allowed(target)
            )
        )

        if not targets:
            raise StopIteration()

        return (targets, distance, distance in self._closest_distances[point])

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

    def __init__(
        self,
        k: int,
        fuel: int,
        matrix: Matrix,
        hint: Sequence[int] = tuple(),
        initial_distance: Optional[int] = None
    ) -> None:
        self._log = get_logger(self)

        self._k = k
        self._fuel = fuel
        self._distance = 0
        self._matrix = matrix
        self._hint = hint
        self._hint_used = False
        self._initial_distance = initial_distance

        self._route: list[int] = []
        self._alternatives: list[list[int]] = []
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

                route_step = len(self._route)
                hint_length = len(self._hint)
                if not self._hint_used and hint_length >= route_step + 1:
                    point = self._hint[route_step]
                    if self._matrix.is_used(point):
                        raise Exception("Hint point already used")

                    distance = self._matrix.distance(self.get_position(), point)
                else:
                    self._hint_used = True
                    points, distance, closest = self._matrix.nearest_point(
                        self.get_position(),
                        self._fuel - self._distance,
                        PointRestriction.get(
                            self._cargo, self._capacity)
                            if not self._unloading
                            else PointRestriction.M,
                        self._initial_distance if len(self._route) == 0 else None
                    )

                    point = points[0]
                    if len(points) > 2 and closest:
                        self._alternatives.extend(a + [b] for a, b in product([self._route], points[1:]))

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

    def get_alternatives(self):
        return tuple(
            alternative
            for alternative in self._alternatives
            if sum(
                self._matrix.distance(alternative[index], alternative[index+1])
                for index in range(len(alternative) - 1)
            ) / self._fuel < 0.15
        )

    def get_matrix(self):
        return self._matrix

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

    def unload(self, to: int = -1):
        if not self._route and to < 0:
            return

        if to >= len(self._route) - 1:
            raise Exception("Wrong unload index")

        while len(self._route) != to + 1:
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

    def _unload_one(self):
        while True:
            position = self.get_position()
            self.unload(len(self._route) - 2)

            if self._matrix.is_n_point(position):
                break

    def __lt__(self, other: Any):
        if not isinstance(other, Truck):
            raise NotImplementedError()

        if self._order == self.Order.FUEL:
            return self._fuel < other.get_fuel()
        else:
            return self._k < other.get_index()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._k}, {self._fuel}, {self._matrix}"

    def __str__(self) -> str:
        return f"[{self._k}]: {self._fuel}"
