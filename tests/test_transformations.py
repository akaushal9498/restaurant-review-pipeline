import unittest
import pandas as pd
from transformation.clean_swiggy import df_clean
from transformation.clean_amazon import cleaned_df

class TestTransformations(unittest.TestCase):

    def test_clean_swiggy(self):
        raw_data = pd.DataFrame({
            'name': ['Taco Bell', 'Dominos', 'Taco Bell'],
            'location': ['Delhi', 'Mumbai', 'Delhi'],
            'rating': [4.2, None, 4.2],
            'date_added': ['2021/01/01', '2021-01-02', '2021/01/01']
        })
        cleaned = df_clean(raw_data)
        self.assertEqual(len(cleaned), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned['date_added']))
        self.assertFalse(cleaned.isnull().any().any())

    def test_clean_amazon(self):
        raw_data = pd.DataFrame({
            'Id': [1, 2, 2],
            'ProductId': ['B001', 'B002', 'B002'],
            'UserId': ['U1', 'U2', 'U2'],
            'Score': [5, None, 5],
            'Time': [1283472000, 1283558400, 1283558400],
            'Summary': ['Great', 'Bad', 'Bad']
        })
        cleaned = cleaned_df(raw_data)
        self.assertEqual(len(cleaned), 2)
        self.assertFalse(cleaned.isnull().any().any())

if __name__ == '__main__':
    unittest.main()
