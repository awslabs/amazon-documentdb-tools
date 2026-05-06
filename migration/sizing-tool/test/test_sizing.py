import unittest
import os
import csv
import tempfile
from unittest.mock import Mock, patch, MagicMock
from argparse import Namespace
import sys

# Import functions from sizing.py (parent directory)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from sizing import (
    validate_args,
    parse_compression_csv,
    run_compression_and_get_output,
    generate_sizing_csv,
    load_compression_module
)


class TestValidateArgs(unittest.TestCase):
    """Tests for validate_args function"""
    
    def test_valid_args(self):
        """Test that valid arguments pass validation"""
        args = Namespace(
            uri='mongodb://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=100
        )
        # Should not raise any exception
        validate_args(args)
    
    def test_valid_args_with_srv(self):
        """Test that mongodb+srv:// URI is valid"""
        args = Namespace(
            uri='mongodb+srv://cluster.mongodb.net',
            sample_size=1000,
            dictionary_sample_size=100
        )
        validate_args(args)
    
    def test_empty_uri(self):
        """Test that empty URI raises ValueError"""
        args = Namespace(
            uri='',
            sample_size=1000,
            dictionary_sample_size=100
        )
        with self.assertRaisesRegex(ValueError, "MongoDB URI cannot be empty"):
            validate_args(args)
    
    def test_invalid_uri_format(self):
        """Test that invalid URI format raises ValueError"""
        args = Namespace(
            uri='http://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=100
        )
        with self.assertRaisesRegex(ValueError, "must start with 'mongodb://' or 'mongodb\\+srv://'"):
            validate_args(args)
    
    def test_negative_sample_size(self):
        """Test that negative sample size raises ValueError"""
        args = Namespace(
            uri='mongodb://localhost:27017',
            sample_size=-100,
            dictionary_sample_size=100
        )
        with self.assertRaisesRegex(ValueError, "Sample size must be positive"):
            validate_args(args)
    
    def test_zero_sample_size(self):
        """Test that zero sample size raises ValueError"""
        args = Namespace(
            uri='mongodb://localhost:27017',
            sample_size=0,
            dictionary_sample_size=100
        )
        with self.assertRaisesRegex(ValueError, "Sample size must be positive"):
            validate_args(args)
    
    def test_negative_dictionary_sample_size(self):
        """Test that negative dictionary sample size raises ValueError"""
        args = Namespace(
            uri='mongodb://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=-10
        )
        with self.assertRaisesRegex(ValueError, "Dictionary sample size must be positive"):
            validate_args(args)
    
    def test_large_values_accepted(self):
        """Test that large values are accepted (no upper limits)"""
        args = Namespace(
            uri='mongodb://localhost:27017',
            sample_size=10000000,  # 10 million
            dictionary_sample_size=5000000  # 5 million
        )
        # Should not raise any exception
        validate_args(args)


class TestParseCompressionCsv(unittest.TestCase):
    """Tests for parse_compression_csv function"""
    
    def test_parse_valid_csv(self):
        """Test parsing a valid compression CSV"""
        csv_content = """compressor,docsSampled,dictDocsSampled,dictBytes
zstd-3-dict,1000,100,4096

dbName,collName,numDocs,avgDocSize,sizeGB,storageGB,existingCompRatio,compEnabled,minSample,maxSample,avgSample,minComp,maxComp,avgComp,projectedCompRatio,exceptions,compTime(ms)
testdb,users,10000,512,5.0,2.5,2.0,Y/1024,256,1024,512,128,512,256,2.0,0,123.45
testdb,orders,5000,1024,5.0,2.0,2.5,Y/1024,512,2048,1024,256,1024,512,2.0,0,234.56
"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            result = parse_compression_csv(temp_file)
            
            self.assertEqual(len(result), 2)
            self.assertIn('testdb.users', result)
            self.assertIn('testdb.orders', result)
            
            users_data = result['testdb.users']
            self.assertEqual(users_data['db_name'], 'testdb')
            self.assertEqual(users_data['coll_name'], 'users')
            self.assertEqual(users_data['num_docs'], 10000)
            self.assertEqual(users_data['avg_doc_size'], 512)
            self.assertEqual(users_data['comp_ratio'], 2.0)
            
            orders_data = result['testdb.orders']
            self.assertEqual(orders_data['db_name'], 'testdb')
            self.assertEqual(orders_data['coll_name'], 'orders')
            self.assertEqual(orders_data['num_docs'], 5000)
        finally:
            os.unlink(temp_file)
    
    def test_parse_csv_missing_header(self):
        """Test that missing header raises RuntimeError"""
        csv_content = """compressor,docsSampled,dictDocsSampled,dictBytes
zstd-3-dict,1000,100,4096

testdb,users,10000,512,5.0,2.5,2.0,Y/1024,256,1024,512,128,512,256,2.0,0,123.45
"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            with self.assertRaisesRegex(RuntimeError, "Could not find data header in CSV"):
                parse_compression_csv(temp_file)
        finally:
            os.unlink(temp_file)
    
    def test_parse_csv_with_invalid_row(self):
        """Test that invalid rows are skipped with warning"""
        csv_content = """compressor,docsSampled,dictDocsSampled,dictBytes
zstd-3-dict,1000,100,4096

dbName,collName,numDocs,avgDocSize,sizeGB,storageGB,existingCompRatio,compEnabled,minSample,maxSample,avgSample,minComp,maxComp,avgComp,projectedCompRatio,exceptions,compTime(ms)
testdb,users,10000,512,5.0,2.5,2.0,Y/1024,256,1024,512,128,512,256,2.0,0,123.45
testdb,invalid,not_a_number,512,5.0,2.5,2.0,Y/1024,256,1024,512,128,512,256,2.0,0,123.45
testdb,orders,5000,1024,5.0,2.0,2.5,Y/1024,512,2048,1024,256,1024,512,2.0,0,234.56
"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            result = parse_compression_csv(temp_file)
            
            # Should have 2 valid rows (invalid row skipped)
            self.assertEqual(len(result), 2)
            self.assertIn('testdb.users', result)
            self.assertIn('testdb.orders', result)
            self.assertNotIn('testdb.invalid', result)
        finally:
            os.unlink(temp_file)
    
    def test_parse_empty_csv(self):
        """Test parsing an empty CSV"""
        csv_content = """compressor,docsSampled,dictDocsSampled,dictBytes
zstd-3-dict,1000,100,4096

dbName,collName,numDocs,avgDocSize,sizeGB,storageGB,existingCompRatio,compEnabled,minSample,maxSample,avgSample,minComp,maxComp,avgComp,projectedCompRatio,exceptions,compTime(ms)
"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            result = parse_compression_csv(temp_file)
            self.assertEqual(len(result), 0)
        finally:
            os.unlink(temp_file)


class TestLoadCompressionModule(unittest.TestCase):
    """Tests for load_compression_module function"""
    
    def test_load_module_file_not_found(self):
        """Test that missing compression module raises RuntimeError"""
        with patch('sizing.os.path.exists', return_value=False):
            with self.assertRaisesRegex(RuntimeError, "Compression module not found"):
                load_compression_module()
    
    def test_load_module_path_is_directory(self):
        """Test that directory path raises RuntimeError"""
        with patch('sizing.os.path.exists', return_value=True):
            with patch('sizing.os.path.isfile', return_value=False):
                with self.assertRaisesRegex(RuntimeError, "Path exists but is not a file"):
                    load_compression_module()
    
    def test_load_module_invalid_spec(self):
        """Test that invalid module spec raises RuntimeError"""
        with patch('sizing.os.path.exists', return_value=True):
            with patch('sizing.os.path.isfile', return_value=True):
                with patch('sizing.importlib.util.spec_from_file_location', return_value=None):
                    with self.assertRaisesRegex(RuntimeError, "Failed to create module spec"):
                        load_compression_module()
    
    def test_load_module_missing_getdata_function(self):
        """Test that module without getData function raises RuntimeError"""
        mock_module = MagicMock()
        del mock_module.getData  # Remove the getData attribute
        
        with patch('sizing.os.path.exists', return_value=True):
            with patch('sizing.os.path.isfile', return_value=True):
                with patch('sizing.importlib.util.spec_from_file_location') as mock_spec_from_file:
                    mock_spec = MagicMock()
                    mock_spec_from_file.return_value = mock_spec
                    with patch('sizing.importlib.util.module_from_spec', return_value=mock_module):
                        with self.assertRaisesRegex(RuntimeError, "missing required 'getData' function"):
                            load_compression_module()
    
    def test_load_module_success(self):
        """Test successful module loading"""
        mock_module = MagicMock()
        mock_module.getData = MagicMock()
        
        with patch('sizing.os.path.exists', return_value=True):
            with patch('sizing.os.path.isfile', return_value=True):
                with patch('sizing.importlib.util.spec_from_file_location') as mock_spec_from_file:
                    mock_spec = MagicMock()
                    mock_spec_from_file.return_value = mock_spec
                    with patch('sizing.importlib.util.module_from_spec', return_value=mock_module):
                        result = load_compression_module()
                        self.assertEqual(result, mock_module)
                        self.assertTrue(hasattr(result, 'getData'))


class TestRunCompressionAndGetOutput(unittest.TestCase):
    """Tests for run_compression_and_get_output function"""
    
    @patch('sizing.load_compression_module')
    @patch('sizing.glob.glob')
    def test_successful_compression_run(self, mock_glob, mock_load_compression):
        """Test successful compression analysis run"""
        # Setup mocks
        mock_compression_module = MagicMock()
        mock_load_compression.return_value = mock_compression_module
        
        mock_glob.side_effect = [
            [],  # No existing files
            ['temp-20260209120000-compression-review.csv']  # New file created
        ]
        
        result = run_compression_and_get_output(
            uri='mongodb://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=100
        )
        
        self.assertEqual(result, 'temp-20260209120000-compression-review.csv')
        mock_compression_module.getData.assert_called_once()
        mock_load_compression.assert_called_once()
    
    @patch('sizing.load_compression_module')
    @patch('sizing.glob.glob')
    def test_compression_run_with_existing_files(self, mock_glob, mock_load_compression):
        """Test compression run when old files exist"""
        # Setup mocks
        mock_compression_module = MagicMock()
        mock_load_compression.return_value = mock_compression_module
        
        mock_glob.side_effect = [
            ['temp-20260209110000-compression-review.csv'],  # Existing file
            [
                'temp-20260209110000-compression-review.csv',
                'temp-20260209120000-compression-review.csv'
            ]  # Old + new file
        ]
        
        result = run_compression_and_get_output(
            uri='mongodb://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=100
        )
        
        self.assertEqual(result, 'temp-20260209120000-compression-review.csv')
    
    @patch('sizing.load_compression_module')
    @patch('sizing.glob.glob')
    def test_compression_run_no_file_created(self, mock_glob, mock_load_compression):
        """Test error when no CSV file is created"""
        # Setup mocks
        mock_compression_module = MagicMock()
        mock_load_compression.return_value = mock_compression_module
        
        mock_glob.side_effect = [[], []]
        
        with self.assertRaisesRegex(RuntimeError, "No new CSV file created"):
            run_compression_and_get_output(
                uri='mongodb://localhost:27017',
                sample_size=1000,
                dictionary_sample_size=100
            )
    
    @patch('sizing.load_compression_module')
    @patch('sizing.glob.glob')
    def test_compression_run_failure(self, mock_glob, mock_load_compression):
        """Test error handling when compression analysis fails"""
        mock_compression_module = MagicMock()
        mock_compression_module.getData.side_effect = Exception("Connection failed")
        mock_load_compression.return_value = mock_compression_module
        
        mock_glob.return_value = []
        
        with self.assertRaisesRegex(RuntimeError, "Error running compression analysis"):
            run_compression_and_get_output(
                uri='mongodb://localhost:27017',
                sample_size=1000,
                dictionary_sample_size=100
            )
    
    @patch('sizing.load_compression_module')
    @patch('sizing.glob.glob')
    @patch('sizing.os.path.getmtime')
    def test_multiple_new_files_created(self, mock_getmtime, mock_glob, mock_load_compression):
        """Test handling when multiple new files are created"""
        # Setup mocks
        mock_compression_module = MagicMock()
        mock_load_compression.return_value = mock_compression_module
        
        mock_glob.side_effect = [
            [],  # No existing files
            [
                'temp-20260209120000-compression-review.csv',
                'temp-20260209120001-compression-review.csv'
            ]  # Two new files
        ]
        
        # Mock getmtime to return different times based on filename
        def getmtime_side_effect(filename):
            if '120001' in filename:
                return 2000  # Newer file
            else:
                return 1000  # Older file
        
        mock_getmtime.side_effect = getmtime_side_effect
        
        result = run_compression_and_get_output(
            uri='mongodb://localhost:27017',
            sample_size=1000,
            dictionary_sample_size=100
        )
        
        # Should return the most recent file
        self.assertEqual(result, 'temp-20260209120001-compression-review.csv')
    
    @patch('sizing.load_compression_module')
    def test_compression_module_load_failure(self, mock_load_compression):
        """Test error handling when compression module fails to load"""
        mock_load_compression.side_effect = RuntimeError("Compression module not found")
        
        with self.assertRaisesRegex(RuntimeError, "Compression module not found"):
            run_compression_and_get_output(
                uri='mongodb://localhost:27017',
                sample_size=1000,
                dictionary_sample_size=100
            )


class TestGenerateSizingCsv(unittest.TestCase):
    """Tests for generate_sizing_csv function"""
    
    @patch('sizing.pymongo.MongoClient')
    @patch('sizing.dt.datetime')
    def test_generate_sizing_csv_success(self, mock_datetime, mock_mongo_client):
        """Test successful sizing CSV generation"""
        # Setup mocks
        mock_datetime.now.return_value.strftime.return_value = '20260209120000'
        
        mock_client = MagicMock()
        mock_mongo_client.return_value.__enter__.return_value = mock_client
        
        # Mock MongoDB collStats response
        mock_client.__getitem__.return_value.command.return_value = {
            'nindexes': 3,
            'totalIndexSize': 1073741824  # 1GB
        }
        
        comp_data = {
            'testdb.users': {
                'db_name': 'testdb',
                'coll_name': 'users',
                'num_docs': 10000,
                'avg_doc_size': 512,
                'comp_ratio': 2.0
            }
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            
            result = generate_sizing_csv(
                comp_data=comp_data,
                uri='mongodb://localhost:27017'
            )
            
            self.assertEqual(result, 'sizing-20260209120000.csv')
            self.assertTrue(os.path.exists(result))
            
            # Verify CSV content
            with open(result, 'r') as f:
                reader = csv.reader(f)
                rows = list(reader)
                
                # Check header
                self.assertEqual(rows[0][0], 'SLNo')
                self.assertEqual(rows[0][1], 'Database_Name')
                
                # Check data row
                self.assertEqual(rows[1][0], '1')
                self.assertEqual(rows[1][1], 'testdb')
                self.assertEqual(rows[1][2], 'users')
                self.assertEqual(rows[1][3], '10000')
    
    @patch('sizing.pymongo.MongoClient')
    @patch('sizing.dt.datetime')
    def test_generate_sizing_csv_with_error(self, mock_datetime, mock_mongo_client):
        """Test sizing CSV generation with collection error"""
        # Setup mocks
        mock_datetime.now.return_value.strftime.return_value = '20260209120000'
        
        mock_client = MagicMock()
        mock_mongo_client.return_value.__enter__.return_value = mock_client
        
        # Mock MongoDB collStats to raise exception
        mock_client.__getitem__.return_value.command.side_effect = Exception("Collection not found")
        
        comp_data = {
            'testdb.users': {
                'db_name': 'testdb',
                'coll_name': 'users',
                'num_docs': 10000,
                'avg_doc_size': 512,
                'comp_ratio': 2.0
            }
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            
            result = generate_sizing_csv(
                comp_data=comp_data,
                uri='mongodb://localhost:27017'
            )
            
            # Should still create file, but with no data rows
            self.assertTrue(os.path.exists(result))
            
            with open(result, 'r') as f:
                reader = csv.reader(f)
                rows = list(reader)
                
                # Only header, no data rows
                self.assertEqual(len(rows), 1)
    
    @patch('sizing.pymongo.MongoClient')
    @patch('sizing.dt.datetime')
    def test_generate_sizing_csv_multiple_collections(self, mock_datetime, mock_mongo_client):
        """Test sizing CSV generation with multiple collections"""
        # Setup mocks
        mock_datetime.now.return_value.strftime.return_value = '20260209120000'
        
        mock_client = MagicMock()
        mock_mongo_client.return_value.__enter__.return_value = mock_client
        
        # Mock MongoDB collStats response
        mock_client.__getitem__.return_value.command.return_value = {
            'nindexes': 2,
            'totalIndexSize': 536870912  # 512MB
        }
        
        comp_data = {
            'testdb.users': {
                'db_name': 'testdb',
                'coll_name': 'users',
                'num_docs': 10000,
                'avg_doc_size': 512,
                'comp_ratio': 2.0
            },
            'testdb.orders': {
                'db_name': 'testdb',
                'coll_name': 'orders',
                'num_docs': 5000,
                'avg_doc_size': 1024,
                'comp_ratio': 2.5
            }
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            
            result = generate_sizing_csv(
                comp_data=comp_data,
                uri='mongodb://localhost:27017'
            )
            
            with open(result, 'r') as f:
                reader = csv.reader(f)
                rows = list(reader)
                
                # Header + 2 data rows
                self.assertEqual(len(rows), 3)
                self.assertEqual(rows[1][2], 'users')
                self.assertEqual(rows[2][2], 'orders')


if __name__ == '__main__':
    unittest.main()
