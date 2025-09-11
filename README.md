
# pypg-iam

Python library for [pg-iam](https://github.com/unioslo/pg-iam).

# Running tests

```bash
poetry install

# set postgres environment variables for pg-iam db access
export PYPGIAM_USER=""
export PYPGIAM_PW=""
export PYPGIAM_HOST=""
export PYPGIAM_DB=""

# and the run tests
poetry run pytest iam/tests.py
```

# LICENSE

BSD.
