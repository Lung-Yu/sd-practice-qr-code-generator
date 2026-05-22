import bisect
import enum
import hashlib


class HashStrategy(enum.Enum):
    RING = "ring"
    RENDEZVOUS = "rendezvous"


class HashRing:
    """Consistent hash ring with optional rendezvous (HRW) fallback."""

    def __init__(self, virtual_nodes: int = 150, strategy: HashStrategy = HashStrategy.RING) -> None:
        self._ring: list[int] = []              # sorted virtual-node positions
        self._ring_map: dict[int, str] = {}     # position → physical node id
        self._nodes: set[str] = set()           # physical node ids
        self.virtual_nodes = virtual_nodes
        self.strategy = strategy

    @staticmethod
    def _hash(s: str) -> int:
        return int(hashlib.md5(s.encode()).hexdigest(), 16)

    # ── pre-written helpers ──────────────────────────────────────────────

    def remove_node(self, node_id: str) -> None:
        """Remove all virtual nodes for node_id from the ring."""
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}:{i}")
            if h in self._ring_map:
                idx = bisect.bisect_left(self._ring, h)
                if idx < len(self._ring) and self._ring[idx] == h:
                    self._ring.pop(idx)
                del self._ring_map[h]
        self._nodes.discard(node_id)

    def node_for_key(self, key: str) -> str:
        """Return the owning node using the configured strategy."""
        if self.strategy == HashStrategy.RENDEZVOUS:
            return self.rendezvous_node(key)
        return self.get_node(key)

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    @property
    def virtual_count(self) -> int:
        return len(self._ring)

    # ── TODOs for guided track ────────────────────────────────────────────

    def add_node(self, node_id: str) -> None:
        """
        TODO: Add a physical node to the ring by inserting its virtual nodes.

        For each i in range(self.virtual_nodes):
          1. Compute h = self._hash(f"{node_id}:{i}")
          2. Use bisect.insort(self._ring, h) to keep the ring sorted
          3. Store self._ring_map[h] = node_id
        Also add node_id to self._nodes.
        """
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}:{i}")
            bisect.insort(self._ring, h)
            self._ring_map[h] = node_id
        self._nodes.add(node_id)

    def get_node(self, key: str) -> str:
        """
        TODO: Find the node that owns this key using the consistent hash ring.

        Algorithm:
        1. If self._ring is empty → raise ValueError("Hash ring is empty")
        2. h = self._hash(key)
        3. idx = bisect.bisect_right(self._ring, h) % len(self._ring)
        4. return self._ring_map[self._ring[idx]]
        """
        if not self._ring:
            raise ValueError("Hash ring is empty")
        h = self._hash(key)
        idx = bisect.bisect_right(self._ring, h) % len(self._ring)
        return self._ring_map[self._ring[idx]]

    def rendezvous_node(self, key: str) -> str:
        """
        TODO: Find the node with the highest HRW score for this key.

        Algorithm (highest random weight):
        1. If self._nodes is empty → raise ValueError("No nodes in ring")
        2. For each node_id in self._nodes, compute score = self._hash(f"{node_id}:{key}")
        3. Return the node_id that has the maximum score
        """
        if not self._nodes:
            raise ValueError("No nodes in ring")
        return max(self._nodes, key=lambda node_id: self._hash(f"{node_id}:{key}"))
