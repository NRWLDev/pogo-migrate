import contextlib
import itertools

import pytest

from pogo_migrate import topologicalsort


class TestTopologicalSort:
    def check(self, nodes, edges, expected):
        deps: dict[str, set[str]] = {}
        edges = edges.split() if edges else []
        for a, b in edges:
            deps.setdefault(a, set()).add(b)
        output = list(topologicalsort.topological_sort(nodes, deps))
        for a, b in edges:
            with contextlib.suppress(ValueError):
                assert output.index(a) > output.index(b)
        assert output == list(expected)

    def test_it_keeps_stable_order(self):
        for s in map(str, itertools.permutations("ABCD")):
            self.check(s, "", s)

    def test_it_sorts_topologically(self):
        # Single group at start
        self.check("ABCD", "BA", "ABCD")  # pragma: no-spell-check
        self.check("BACD", "BA", "ABCD")  # pragma: no-spell-check

        # Single group in middle start
        self.check("CABD", "BA", "CABD")  # pragma: no-spell-check
        self.check("CBAD", "BA", "CABD")  # pragma: no-spell-check

        # Extended group
        self.check("ABCD", "BA DA", "ABCD")  # pragma: no-spell-check
        self.check("DBCA", "BA DA", "CADB")  # pragma: no-spell-check

        # Non-connected groups
        self.check("ABCDEF", "BC DE", "ACBEDF")
        self.check("ADEBCF", "BC DE", "AEDCBF")
        self.check("ADEFBC", "BC DE", "AEDFCB")
        self.check("DBAFEC", "BC DE", "AFEDCB")  # pragma: no-spell-check

    def test_it_discards_missing_dependencies(self):
        self.check("ABCD", "CX XY", "ABCD")

    def test_it_catches_cycles(self):
        with pytest.raises(topologicalsort.CycleError):
            self.check("ABCD", "AA", "")
        with pytest.raises(topologicalsort.CycleError):
            self.check("ABCD", "AB BA", "")  # pragma: no-spell-check
        with pytest.raises(topologicalsort.CycleError):
            self.check("ABCD", "AB BC CB", "")
        with pytest.raises(topologicalsort.CycleError):
            self.check("ABCD", "AB BC CA", "")

    def test_it_handles_multiple_edges_to_the_same_node(self):
        self.check("ABCD", "BA CA DA", "ABCD")  # pragma: no-spell-check
        self.check("DCBA", "BA CA DA", "ADCB")  # pragma: no-spell-check

    def test_it_handles_multiple_edges_to_the_same_node2(self):
        #      A --> B
        #      |     ^
        #      v     |
        #      C --- +
        for input_order in itertools.permutations("ABC"):
            self.check(input_order, "BA CA BC", "ACB")  # pragma: no-spell-check

    def test_it_doesnt_modify_order_unnecessarily(self):
        self.check("ABC", "CA", "ABC")
