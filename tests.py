import unittest
import ite3


class TestPager(unittest.TestCase):
    def test_pager_open(self):
        pager = ite3.pager.Pager()

        pager.open("testdata/empty.sqlite", "r")
        pager.close()

        pager.open("testdata/empty.sqlite", "w")
        pager.close()

        pager.open("testdata/northwind.sqlite", "w")
        pager.close()

        with self.assertRaises(ValueError):
            pager.open("testdata/zerolength.sqlite", "w")


class TestIte3(unittest.TestCase):
    def test_pager_open(self):
        io1 = ite3.open("testdata/empty.sqlite", "r")
        io1.close()
        io2 = ite3.open("testdata/empty.sqlite", "w")
        io2.close()
            

if __name__ == "__main__":
    unittest.main()
