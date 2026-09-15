import unittest

from primary_node import split_into_slices
from primes_in_range import get_primes
from secondary_node import compute_partitioned, iter_ranges


class PrimeTests(unittest.TestCase):
    def test_small_range(self):
        expected = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        self.assertEqual(get_primes(0, 30), expected)
        self.assertEqual(get_primes(0, 30, return_list=False), len(expected))

    def test_empty_and_negative_ranges(self):
        self.assertEqual(get_primes(-20, 2), [])
        self.assertEqual(get_primes(10, 10), [])
        self.assertEqual(get_primes(10, 5, return_list=False), 0)


class PartitionTests(unittest.TestCase):
    def test_chunk_ranges_cover_full_interval(self):
        self.assertEqual(iter_ranges(3, 14, 4), [(3, 7), (7, 11), (11, 14)])

    def test_primary_slices_are_balanced(self):
        slices = split_into_slices(0, 10, 3)
        self.assertEqual(slices, [(0, 4), (4, 7), (7, 10)])

    def test_partitioned_count(self):
        result = compute_partitioned(0, 100, chunk=13, exec_mode="single")
        self.assertEqual(result["total_primes"], 25)
        self.assertEqual(result["chunks"], 8)

    def test_partitioned_list_limit(self):
        result = compute_partitioned(
            0,
            100,
            mode="list",
            chunk=20,
            exec_mode="threads",
            workers=2,
            max_return_primes=5,
        )
        self.assertEqual(result["total_primes"], 25)
        self.assertEqual(result["primes"], [2, 3, 5, 7, 11])
        self.assertTrue(result["primes_truncated"])


if __name__ == "__main__":
    unittest.main()
