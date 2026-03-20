MIN_CHUNK: int = 1048576  # 1MB
STREAM_CHUNK_SIZE: int = 5242880  # 5MB

HTTP_TOO_MANY_REQUESTS: int = 429
HTTP_BAD_REQUEST_THRESHOLD: int = 400

# AMID Scaling Logic
SCALE_UP_PROBABILITY: float = 0.1  # For the 10% chance to try_scale_up

# Validation
MD5_HEX_LENGTH: int = 32

# Hash File Parsing (NCBI style)
HASH_FILE_MIN_PARTS: int = 2

# Storage Units
ONE_GIBIBYTE: int = 1_073_741_824

# Time Conversion
MINUTES_PER_HOUR: int = 60

# The transition point for gradient calculations
COLOR_PIVOT_PERCENT: float = 50.0
