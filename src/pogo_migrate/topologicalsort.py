# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from heapq import heappop, heappush
from typing import TypeVar


class CycleError(ValueError):
    """
    Raised when cycles exist in the input graph.

    The second element in the args attribute of instances will contain the
    sequence of nodes in which the cycle lies.
    """


T = TypeVar("T")


def topological_sort(
    items: Iterable[T],
    dependency_graph: Mapping[T, Collection[T]],
) -> Iterable[T]:
    # Tag each item with its input order
    pqueue = list(enumerate(items))
    ordering = {item: ix for ix, item in pqueue}
    seen_since_last_change = 0
    output: set[T] = set()

    # Map blockers to the list of items they block
    blocked_on: dict[T, set[T]] = defaultdict(set)
    blocked: set[T] = set()

    while pqueue:
        if seen_since_last_change == len(pqueue) + len(blocked):
            raise_cycle_error(ordering, pqueue, blocked_on)

        _, n = heappop(pqueue)

        blockers = {d for d in dependency_graph.get(n, []) if d not in output and d in ordering}
        if not blockers:
            seen_since_last_change = 0
            output.add(n)
            if n in blocked:
                blocked.remove(n)
            yield n
            for b in blocked_on.pop(n, []):
                if not any(b in other for other in blocked_on.values()):
                    heappush(pqueue, (ordering[b], b))
        elif n in blocked:
            seen_since_last_change += 1
        else:
            seen_since_last_change = 0
            blocked.add(n)
            for b in blockers:
                blocked_on[b].add(n)
    if blocked_on:
        raise_cycle_error(ordering, pqueue, blocked_on)


def raise_cycle_error(ordering: dict[T, int], pqueue: list[tuple[int, T]], blocked_on: dict[T, set[T]]) -> None:
    bad = next((item for item in blocked_on if item not in ordering), None)
    if bad:
        msg = f"Dependency graph contains a non-existent node {bad!r}"
        raise ValueError(msg)
    unresolved = {n for _, n in pqueue}
    unresolved.update(*blocked_on.values())
    if unresolved:
        msg = f"Dependency graph loop detected among {unresolved!r}"
        raise CycleError(
            msg,
            sorted(unresolved, key=ordering.get),
        )
    msg = "raise_cycle_error called but no unresolved nodes exist"
    raise AssertionError(msg)
