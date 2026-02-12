# Sizing Tool Tests

This directory contains unit tests for the sizing tool.

## Prerequisites

- Python 3.7+
- No external dependencies required (tests use `unittest.mock` for all external calls)
- Tests do not require MongoDB connection or the compression-review.py script

## Running Tests

### Run all tests
```bash
# From the test directory
python -m unittest test_sizing

# With verbose output
python -m unittest test_sizing -v
```

### Run specific test class
```bash
python -m unittest test_sizing.TestValidateArgs
```

### Run specific test
```bash
python -m unittest test_sizing.TestValidateArgs.test_valid_args
```

## Test Coverage

The test suite includes unit tests for:

- **Argument validation** - URI format, sample sizes, parameter bounds
- **CSV parsing** - Valid data, missing headers, invalid rows, empty files
- **Compression module loading** - File existence, module validation, error handling
- **Compression execution** - Successful runs, file creation, error scenarios, cleanup
- **Sizing CSV generation** - MongoDB stats collection, multiple collections, error handling

## Test Structure

All tests use mocks to avoid external dependencies:
- MongoDB connections are mocked using `unittest.mock`
- File system operations use temporary files
- The compression-review.py module is mocked for isolation

This ensures tests run quickly and don't require any external services or configuration.

## Adding New Tests

When adding new functionality to sizing.py:

1. Create a new test class or add to an existing one
2. Use descriptive test names that explain what is being tested
3. Mock all external dependencies (MongoDB, file system, external modules)
4. Test both success and failure scenarios
5. Include edge cases and boundary conditions

Example test structure:
```python
class TestNewFeature(unittest.TestCase):
    """Tests for new_feature function"""
    
    @patch('sizing.external_dependency')
    def test_success_case(self, mock_dependency):
        """Test successful execution"""
        # Setup mocks
        mock_dependency.return_value = expected_value
        
        # Execute
        result = new_feature()
        
        # Assert
        self.assertEqual(result, expected_value)
```
