import re


QUERY_NAME_DEFINITION_PATTERN = re.compile(r"--\s*name\s*:\s*")
DOC_COMMENT_PATTERN = re.compile(r"\s*--\s*(.*)$")
VALID_QUERY_NAME_PATTERN = re.compile(r"^\w+$")

WRONG_PARAMS = re.compile(
    r"the\squery\shas\s\d+\splaceholders\sbut\s\d+\sparameters\swere\spassed"
)
