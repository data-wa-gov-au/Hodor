class QueryTooExpensive(Exception):
  pass

class BackendError(Exception):
  pass

class QPSTooLow(Exception):
  pass

class TableTooLarge(Exception):
  pass

class InternalServerError(Exception):
  pass
