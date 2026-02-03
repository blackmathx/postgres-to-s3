from sqlalchemy import create_engine
import unittest
import os, sys 


# order matters and is needed to import from src folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))  


from config.env_reader import EnvReader
from extract import extract_genre
from transform import gen_transform
from extract import extract_batch


class TestMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ setup to get connection string for the class"""
        DB_USER = EnvReader.get("DB_USER")
        DB_PASSWORD = EnvReader.get("DB_PASSWORD")
        DB_HOST = EnvReader.get("DB_HOST")
        DB_NAME = EnvReader.get("DB_NAME")
       
        cls.conn_str = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"


    def test_envreader(self):
        """ Test EnvReader for environment variables """
        val = EnvReader.get("DB_USER")
        self.assertEqual(val, "postgres")


    def test_batch_extract_and_general_transform(self):
        """ test the batch extract and the general transform methods """
        engine = create_engine(self.conn_str)

        raw_df = extract_batch(engine, "genre", "genre_id", 200, 0) 
        clean_df = gen_transform(raw_df, "genre_id")
        engine.dispose( )
        
        self.assertFalse(raw_df.empty)
        self.assertFalse(clean_df.empty)




if __name__ == '__main__':
    unittest.main()