#!/usr/bin/env python3

import unittest
import csv

from etl_script import ExtractTransformLoad


class TestETL(unittest.TestCase):

    def setUp(self):
        self.etl = ExtractTransformLoad()
        self.etl.read_csv('data/test_data.csv')

    def test_len_columns(self):
        """
        Checking the specific length of a set with column names
        """
        self.assertEqual(len(self.etl.columns), 7)

    def test_val_in_row(self):
        """
        Checking for a specific value in a row
        """
        row = self.etl.rows[2]
        val = row.get('M3')
        self.assertEqual(val, 5)

    def test_val_in_out_file(self):
        """
        Checking the sum of values across all rows and along the diagonal of
        columns from M1 to Mn in the output file
        """
        self.etl.write_tsv_basic('results/test_results.tsv',
                                 common_columns=False)
        with open('results/test_results.tsv') as f:
            reader = csv.DictReader(f, delimiter='\t')
            cnt = 1
            values = []
            for row in reader:
                values.append(int(row[f'M{str(cnt)}']))
                cnt += 1
        self.assertEqual(sum(values), 19)


if __name__ == '__main__':
    unittest.main()
