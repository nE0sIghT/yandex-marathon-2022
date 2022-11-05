import random
from datetime import datetime
from typing import Any, Optional, Sequence

from .logs import get_logger
from .objects import Matrix, Truck


class TruckProcessor:
    def __init__(self) -> None:
        self._logs = get_logger(__name__)
        self.mp_results: dict[int, dict[int, list[int]]] = {}

    def iteration(
        self,
        truck_ids: tuple[int],
        n: int,
        m: int,
        matrix_raw: tuple[tuple[int]],
        restrictions: tuple[int]
    ):
        def process_truck(
            truck: Truck,
            alternatives: list[Sequence[int]],
            all_alternatives: set[int],
            best_result: int
        ):
            def alternative_used(element: int):
                if not element in all_alternatives:
                    all_alternatives.add(element)
                    return False

                return True

            result = None
            route = None

            truck.calculate()

            matrix = truck.get_matrix()
            if best_result < matrix.get_used_n_points() and truck.get_route():
                result = matrix.get_used_n_points()
                route = truck.get_route().copy()

            new_alternatives = tuple(
                a for a in truck.get_alternatives()
                if not alternative_used(hash(tuple(a)))
            )
            alternatives.extend(new_alternatives)

            truck.unload()

            return result, route

        matrix = Matrix(n, m, matrix_raw)
        trucks_routes: dict[int, list[int]] = {}
        for index in truck_ids:
            alternatives: list[Sequence[int]] = []
            all_alternatives: set[int] = set()
            best_result = 0
            best_route: list[int] = []

            for initial_distance in list(matrix.get_initial_closest_distances()) + [None]:
                result, route = process_truck(
                    Truck(index, restrictions[index], matrix, tuple(), initial_distance),
                    alternatives,
                    all_alternatives,
                    best_result
                )
                if result and route:
                    best_result = result
                    best_route = route

            _start = datetime.now()
            counter = 0
            while alternatives:
                if counter % 3 == 0:
                    i = 0
                elif counter % 2 == 0:
                    i = -1
                else:
                    i = random.randrange(len(alternatives))

                result, route = process_truck(
                    Truck(index, restrictions[index], matrix, alternatives.pop(i)),
                    alternatives,
                    all_alternatives,
                    best_result
                )
                if result and route:
                    best_result = result
                    best_route = route

                if (datetime.now() - _start).total_seconds() > 7:
                    break

                counter += 1

            trucks_routes[index] = best_route

            truck = Truck(index, restrictions[index], matrix, trucks_routes[index])
            truck.calculate()

        return matrix.get_used_n_points(), trucks_routes


    def on_success(self, task_results: tuple[int, dict[int, list[int]]]):
        self.mp_results.update({task_results[0]: task_results[1]})

    def on_error(self, arg: Any):
        self._logs.error(arg)

    def rearm(self):
        self.mp_results.clear()

    def results(self):
        return self.mp_results.copy()
