import unittest
import etl_process as etl
import datetime
import pandas as pd


class TestETLProcess(unittest.TestCase):
    def test_extract(self):
        test_copyintodate = '05/01/2019'
        test_filename = 'Fake Family Medicine 050119'
        test_providergroup = 'Fake Family Medicine'
        expected_dataframe = {
            'ID': 12275,
            'First Name': 'Jane',
            'Middle Name': 'Christine',
            'Last Name': 'Doe',
            'DOB[1]': '1987-11-18 00:00:00',
            'Sex': 0,
            'Favorite Color': 'Blue',
            'Attributed Q1': 'Yes',
            'Attributed Q2': 'Yes',
            'Risk Q1': 0.4445286866540108,
            'Risk Q2 ': 0.4625272036310827,
            'Risk Increased Flag': 'Yes'
        }

        result = etl.extract(test_copyintodate, test_filename, test_providergroup)

        self.assertEqual(result['ID'][0], expected_dataframe['ID'])
        self.assertEqual(result['First Name'][0], expected_dataframe['First Name'])
        self.assertEqual(result['Middle Name'][0], expected_dataframe['Middle Name'])
        self.assertEqual(result['Last Name'][0], expected_dataframe['Last Name'])
        self.assertEqual(result['DOB[1]'][0], datetime.datetime.strptime(expected_dataframe['DOB[1]']
                                                                         ,'%Y-%m-%d %H:%M:%S'))
        self.assertEqual(result['Sex'][0], expected_dataframe['Sex'])
        self.assertEqual(result['Favorite Color'][0], expected_dataframe['Favorite Color'])
        self.assertEqual(result['Attributed Q1'][0], expected_dataframe['Attributed Q1'])
        self.assertEqual(result['Attributed Q2'][0], expected_dataframe['Attributed Q2'])
        self.assertEqual(result['Risk Q1'][0], expected_dataframe['Risk Q1'])
        self.assertEqual(result['Risk Q2 '][0], expected_dataframe['Risk Q2 '])
        self.assertEqual(result['Risk Increased Flag'][0], expected_dataframe['Risk Increased Flag'])


    def test_transform_demographics(self):
        test_data = { 'test_row': [
            12275,
            'Jane',
            'Christine',
            'Doe',
            datetime.datetime.strptime('1987-11-18 00:00:00','%Y-%m-%d %H:%M:%S'),
            0,
            'Blue',
            'Yes',
            'Yes',
            0.4445286866540108,
            0.4625272036310827,
            'Yes'
        ]}
        expected_dataframe = {
            'ID': 12275,
            'First Name': 'Jane',
            'Middle Name': 'C',
            'Last Name': 'Doe',
            'DOB[1]': '1987-11-18T00:00:00',
            'Sex': 'M',
            'Favorite Color': 'Blue'
        }

        test_df = pd.DataFrame.from_dict(test_data, orient='index', columns=['ID', 'First Name', 'Middle Name', 'Last Name', 'DOB[1]', 'Sex'
            , 'Favorite Color', 'Attributed Q1', 'Attributed Q2', 'Risk Q1', 'Risk Q2', 'Risk Increased Flag'])
        result = etl.transform_demographics(test_df)

        self.assertEqual(result['ID'][0], expected_dataframe['ID'])
        self.assertEqual(result['First Name'][0], expected_dataframe['First Name'])
        self.assertEqual(result['Middle Name'][0], expected_dataframe['Middle Name'])
        self.assertEqual(result['Last Name'][0], expected_dataframe['Last Name'])
        self.assertEqual(result['DOB[1]'][0], expected_dataframe['DOB[1]'])
        self.assertEqual(result['Sex'][0], expected_dataframe['Sex'])
        self.assertEqual(result['Favorite Color'][0], expected_dataframe['Favorite Color'])


    def test_transform_riskquarter(self):
        test_data = {'test_row': [
            12275,
            'Jane',
            'Christine',
            'Doe',
            datetime.datetime.strptime('1987-11-18 00:00:00', '%Y-%m-%d %H:%M:%S'),
            0,
            'Blue',
            'Yes',
            'Yes',
            0.4445286866540108,
            0.4625272036310827,
            'Yes'
        ]}
        expected_dataframe = {
            0: {
                'ID': 12275,
                'Quarter': 'Q1',
                'RiskScore': 0.4445286866540108,
                'AttributedFlag': 'Yes'
            },
            1: {
                'ID': 12275,
                'Quarter': 'Q2',
                'RiskScore': 0.4625272036310827,
                'AttributedFlag': 'Yes'
            }
        }

        test_df = pd.DataFrame.from_dict(test_data, orient='index',
                                         columns=['ID', 'First Name', 'Middle Name', 'Last Name', 'DOB[1]', 'Sex'
                                             , 'Favorite Color', 'Attributed Q1', 'Attributed Q2', 'Risk Q1', 'Risk Q2 ',
                                                  'Risk Increased Flag'])
        result = etl.transform_riskquarter(test_df)

        self.assertEqual(result['ID'][0], expected_dataframe[0]['ID'])
        self.assertEqual(result['Quarter'][0], expected_dataframe[0]['Quarter'])
        self.assertEqual(result['RiskScore'][0], expected_dataframe[0]['RiskScore'])
        self.assertEqual(result['AttributedFlag'][0], expected_dataframe[0]['AttributedFlag'])

        self.assertEqual(result['ID'][1], expected_dataframe[1]['ID'])
        self.assertEqual(result['Quarter'][1], expected_dataframe[1]['Quarter'])
        self.assertEqual(result['RiskScore'][1], expected_dataframe[1]['RiskScore'])
        self.assertEqual(result['AttributedFlag'][1], expected_dataframe[1]['AttributedFlag'])


if __name__ == '__main__':
    unittest.main()