import warnings
from typing import Optional
import uuid
import duckdb  # type: ignore

class UDFManager:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.UDF_LIST = [
            "generate_id",
            "random_id",
        ]

       
    def generate_id(self, input_string: Optional[str]) -> Optional[int]:
        if input_string is None:
            return None
        else:
            # Specify a constant seed value so function has deterministic behavior
            return hash(input_string) & 0xFFFFFFFFFFFFFF  # Mask to 7 bytes

    def random_id(self) -> str:
        return str(uuid.uuid4())
       
    def register_udfs(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # Registering UDF's with DuckDB raises warning; ignorning it
            # Iterate over the list of function names directly
            for func_name in self.UDF_LIST:
                # Ensure func_name is a string
                if not isinstance(func_name, str):
                    raise ValueError(f"Function name must be a string, got {type(func_name)}")
                # Register function
                try:
                    # Using getattr to dynamically get the method from the class based on the name string
                    # null_handling='special' required when UDFs may return NULL/None
                    self.conn.create_function(func_name, getattr(self, func_name), null_handling='special')
                except AttributeError as e:
                    raise AttributeError(f"Failed to access method {func_name}: {str(e)}")
                except Exception as e:
                    # Catching any other exceptions that might be raised during registration
                    raise Exception(f"Failed to register function {func_name}: {str(e)}")  