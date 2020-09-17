#!/usr/bin/env python3

import csv
import json
import xml.etree.ElementTree as ET


class ExtractTransformLoad:
    """
    The class implements data extraction from files of the following
    formats: .csv, .json, .xml. Transforms the received data and loads
    the converted data into a file in a tabular .tsv format.
    """

    def __init__(self):
        """
        The method initializes the following attributes:
        row - a list into which rows with values in the form of dictionaries
        (where the key is the name of the column, the value is the
        corresponding value of the string or number in the row) are written
        from the data files;
        columns - a set that stores the names of all columns from all data
        files;
        common_columns - a list that stores sets with column names from each
        data file.
        """

        self.rows = []
        self.columns = set()
        self.common_columns = []

    def read_csv(self, path: str):
        """
        The method implements reading data from files in the .csv format.
        Updates attributes according to read data.

        :param path: path to data file in .csv format
        :type path: str

        :raises ValueError: if the value in column M is not a number
        """

        with open(path) as f:
            reader = csv.reader(f)
            columns = None
            for row in reader:
                if not columns:
                    columns = row
                    self.columns.update(row)
                    self.common_columns.append(set(row))
                    continue
                new_row = {}
                for key, val in zip(columns, row):
                    if key[0] == 'D':
                        new_row[key] = val
                    elif key[0] == 'M':
                        try:
                            value = int(val)
                        except ValueError:
                            print(f"Incorrect value: '{val}'\n"
                                  f'file: {path}\n'
                                  f'column: {key}\n'
                                  f'row number: {reader.line_num}\n')
                            continue
                        else:
                            new_row[key] = value

                self.rows.append(new_row)

    def read_json(self, path: str):
        """
        The method implements reading data from files in the .json format.
        Updates attributes according to read data.

        :param path: path to data file in .json format
        :type path: str

        :raises ValueError: if the value in column M is not a number
        """

        with open(path) as f:
            data = json.load(f)
            common_columns = set()
            for field in data['fields']:
                self.columns.update(field.keys())
                new_row = {}
                for key, val in field.items():
                    common_columns.add(key)
                    if key[0] == 'M':
                        try:
                            value = int(val)
                        except ValueError:
                            print(f"Incorrect value: '{val}'\n"
                                  f'file: {path}\n'
                                  f'key: {key}\n'
                                  f'field: {field}\n')
                            continue
                        else:
                            new_row[key] = value
                    new_row[key] = val

                self.rows.append(new_row)
            self.common_columns.append(common_columns)

    def read_xml(self, path: str):
        """
        The method implements reading data from files in the .xml format.
        Updates attributes according to read data.

        :param path: path to data file in .xml format
        :type path: str

        :raises ValueError: if the value in column M is not a number
        """

        tree = ET.parse(path)
        new_row = {}
        common_columns = set()
        for obj in tree.findall('objects/object'):
            column = obj.attrib['name']
            self.columns.add(column)
            common_columns.add(column)
            for val in obj:
                val = val.text
                if column[0] == 'D':
                    new_row[column] = val
                elif column[0] == 'M':
                    try:
                        value = int(val)
                    except ValueError:
                        print(f"Incorrect value: '{val}'\n"
                              f'file: {path}\n'
                              f'object name: {column}\n')
                        continue
                    else:
                        new_row[column] = value

        self.rows.append(new_row)
        self.common_columns.append(common_columns)

    @property
    def d_m_sorted_columns(self):
        """
        The property implements splitting the columns attribute into D- and
        M-columns and sorting each of the received columns.
        Returns 2 tuples with D columns and M columns.
        """

        d_columns = []
        m_columns = []
        for column in self.columns:
            if column[0] == 'D':
                d_columns.append(column)
            elif column[0] == 'M':
                m_columns.append(column)

        d_columns = tuple(sorted(d_columns, key=lambda y: int(y[1:])))
        m_columns = tuple(sorted(m_columns, key=lambda y: int(y[1:])))

        return d_columns, m_columns

    @property
    def d_m_sorted_common_columns(self):
        """
        The property implements splitting into D- and M-columns of the
        intersection of column names in the common_columns attribute and
        sorting each of the resulting columns.
        Returns 2 tuples with D columns and M columns.
        """

        if not self.common_columns:
            return tuple(), tuple()

        d_columns = []
        m_columns = []

        for column in self.common_columns[0].intersection(
                *self.common_columns[1:]):
            if column[0] == 'D':
                d_columns.append(column)
            elif column[0] == 'M':
                m_columns.append(column)

        d_columns = tuple(sorted(d_columns, key=lambda y: int(y[1:])))
        m_columns = tuple(sorted(m_columns, key=lambda y: int(y[1:])))

        return d_columns, m_columns

    def write_tsv_basic(self, path: str, common_columns: bool):
        """
        The method implements the transformation (basic) of the read data
        from all files and writes it to a file in the .tsv format.
        The data in the output file is sorted by column D1.

        :param path: path to output file name in .tsv format
        :type path: str

        :param common_columns: determines the state of the columns in the
        output file:
        if True - the output file will have only those columns that intersect
        in all data files;
        if False - the output file will have all columns that are found in all
        data files
        :type common_columns: bool
        """

        with open(path, 'w') as out_file:
            tsv_writer = csv.writer(out_file, delimiter='\t')
            if common_columns:
                d_columns, m_columns = self.d_m_sorted_common_columns
            else:
                d_columns, m_columns = self.d_m_sorted_columns
            columns = d_columns + m_columns
            tsv_writer.writerow(columns)
            sorted_rows = sorted(self.rows, key=lambda x: x['D1'])
            for row in sorted_rows:
                out_row = []
                for col in columns:
                    out_row.append(row.get(col, '-'))
                tsv_writer.writerow(out_row)

    def get_data_advanced(self, d_columns: tuple, m_columns: tuple) -> dict:
        """
        The method implements data transformation for the
        write_tsv_advanced method. The data is grouped by unique
        values of combinations of rows from columns D1...Dn and
        the sums of the corresponding values from columns M1...Mn
        are calculated.

        :param d_columns: sorted tuple with column names D1...Dn
        :type d_columns: tuple
        :param m_columns: sorted tuple with column names M1...Mn
        :type m_columns: tuple

        :rtype: dict
        :return: dictionary with transformed data,
        where keys are tuples of unique combinations of values from columns
        D1...Dn, values are tuples of corresponding sums of values from
        columns M1...Mn
        """

        data = {}
        default_val = tuple(0 for _ in m_columns)
        for row in self.rows:
            key = tuple(map(lambda x: row.get(x, '-'), d_columns))
            val = tuple(map(lambda x: row.get(x, 0), m_columns))
            val_dict = data.setdefault(key, default_val)
            new_val = tuple(map(lambda x: x[0] + x[1], zip(val_dict, val)))
            data[key] = new_val

        return data

    def write_tsv_advanced(self, path: str, common_columns: bool):
        """
        The method implements the transformation (advanced) of the
        read data from all files and writes it to a file in the .tsv format.
        Columns MS1...MSn contain the sums of the values of the corresponding
        M1...Mn from all files, grouped by the unique values
        of the combinations of lines from D1...Dn.

        :param path: path to output file name in .tsv format
        :type path: str

        :param common_columns: determines the state of the columns in the
        output file:
        if True - the output file will have only those columns that intersect
        in all data files;
        if False - the output file will have all columns that are found in all
        data files
        :type common_columns: bool
        """

        with open(path, 'w') as out_file:
            tsv_writer = csv.writer(out_file, delimiter='\t')
            if common_columns:
                d_columns, m_columns = self.d_m_sorted_common_columns
            else:
                d_columns, m_columns = self.d_m_sorted_columns
            tsv_writer.writerow(d_columns + tuple(map(
                lambda v: f'{v[0]}S{v[1:]}', m_columns)))
            data = self.get_data_advanced(d_columns=d_columns,
                                          m_columns=m_columns)
            for key in sorted(data.keys()):
                tsv_writer.writerow(key + data[key])


if __name__ == '__main__':
    print('Start process')
    etl = ExtractTransformLoad()
    etl.read_csv('data/csv_data_1.csv')
    etl.read_csv('data/csv_data_2.csv')
    etl.read_json('data/json_data.json')
    etl.read_xml('data/xml_data.xml')
    etl.write_tsv_basic('results/out_basic.tsv', common_columns=False)
    etl.write_tsv_advanced('results/out_advanced.tsv', common_columns=False)
    etl.write_tsv_basic('results/out_basic_union.tsv', common_columns=True)
    etl.write_tsv_advanced('results/out_advanced_union.tsv',
                           common_columns=True)
    print('Process completed')
