"""Unit tests for nexuskv-cli diagnostic tool."""

import unittest

from nexuskv.cli import main, print_health, print_nic, print_status


class TestNexusKVCLI(unittest.TestCase):
    def test_print_status(self) -> None:
        ret = print_status()
        self.assertEqual(ret, 0)

    def test_print_nic(self) -> None:
        ret = print_nic()
        self.assertEqual(ret, 0)

    def test_print_health(self) -> None:
        ret = print_health()
        self.assertEqual(ret, 0)

    def test_cli_main_subcommands(self) -> None:
        self.assertEqual(main(["status"]), 0)
        self.assertEqual(main(["nic"]), 0)
        self.assertEqual(main(["health"]), 0)


if __name__ == "__main__":
    unittest.main()
